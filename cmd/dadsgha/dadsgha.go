package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"regexp"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	dads "github.com/LF-Engineering/da-ds"
	lib "github.com/LF-Engineering/da-ds-gha"
	"github.com/google/go-github/github"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	yaml "gopkg.in/yaml.v2"
)

type ghaMapItem struct {
	dt    time.Time
	key   string
	repos map[string]int
	ok    bool
}

// config used across this project is map[[2]string]regexp.RegExp
// keys are: [2]string
// first item is a fixture slug and its data sources configuartion,
// like 'cncf/k8s:pull_request=,issue=,repository=,'
// for each GH index type (issue, PR, repository) there can be a different index suffix
// 2nd item is a project name, for example 'Kubernetes'
// value is a regular expression that matches a given fixture slug/project pair
// something like '(?i)^prometheus$' (compiled)

const (
	// FIXME
	// cPrefix = "gha-"
	cPrefix = "sds-"
	// cMaxGitHubUsersFileCacheAge 90 days (in seconds) - file is considered too old anywhere between 90-180 days
	cMaxGitHubUsersFileCacheAge = 7776000
	// cNDaysGHAPeriod - how many days cache in GHA map files at once
	cNDaysGHAPeriod = 5
	// cAllowedRepoAPIAge - when doin fork event enrichment we use GH API:
	// when more than 1 fork is processed then we skip repeating the same state for 15 minutes, and after that time we invaliadte the cache
	cAllowedRepoAPIAge = 900
)

var (
	gctx                context.Context
	gc                  []*github.Client
	gInit               bool
	gHint               int
	gThrN               int
	gGHAMap             map[string]map[string]int
	gRichMtx            *sync.Mutex
	gEnsuredIndicesMtx  *sync.Mutex
	gUploadMtx          *sync.Mutex
	gUploadDBMtx        *sync.Mutex
	gDocsUploaded       int64
	gGitHubUsersMtx     *sync.RWMutex
	gGitHubMtx          *sync.RWMutex
	gDadsCtx            dads.Ctx
	gIdentityMtx        *sync.Mutex
	gGitHubReposMtx     *sync.RWMutex
	gJSONsBytesMtx      *sync.Mutex
	gJSONsLockMtx       *sync.Mutex
	gJSONsL2Mtx         *sync.Mutex
	gSyncAllDatesMtx    *sync.Mutex
	gJSONsLocked        bool
	gAllJSONsBytes      int64
	gMaxJSONsBytes      int64
	gRichItems          = map[string][]map[string]interface{}{}
	gEnsuredIndices     = map[string]struct{}{}
	gGitHubRichMapping  = `{"properties":{"merge_author_geolocation":{"type":"geo_point"},"assignee_geolocation":{"type":"geo_point"},"state":{"type":"keyword"},"user_geolocation":{"type":"geo_point"},"title_analyzed":{"type":"text","index":true}}}`
	gNewFormatStarts    = time.Date(2015, 1, 1, 0, 0, 0, 0, time.UTC)
	gMinGHA             = time.Date(2014, 1, 1, 0, 0, 0, 0, time.UTC)
	gGitHubUsers        = map[string]map[string]*string{}
	gGitHubRepos        = map[string]map[string]interface{}{}
	gIdentities         = map[[3]string]struct{}{}
	gUploadedIdentities = map[[3]string]struct{}{}
	gGitHubDS           = &dads.DSStub{DS: "github"}
	gSyncDates          = map[string]map[string]time.Time{}
	gSyncAllDates       = map[string]map[string]time.Time{}
)

func processFixtureFile(ch chan lib.Fixture, ctx *lib.Ctx, fixtureFile string) (fixture lib.Fixture) {
	defer func() {
		if ch != nil {
			ch <- fixture
		}
	}()
	// Read defined projects
	data, err := ioutil.ReadFile(fixtureFile)
	if err != nil {
		lib.Printf("Error reading file: %s\n", fixtureFile)
	}
	lib.FatalOnError(err)
	err = yaml.Unmarshal(data, &fixture)
	if err != nil {
		lib.Printf("Error parsing YAML file: %s\n", fixtureFile)
	}
	lib.FatalOnError(err)
	slug := fixture.Native.Slug
	if slug == "" {
		lib.Fatalf("Fixture file %s 'native' property has no 'slug' property (or is empty)\n", fixtureFile)
	}
	if fixture.Disabled == true {
		return
	}
	return
}

func handleRate(ctx *lib.Ctx) (aHint int, canCache bool) {
	h, _, rem, wait := lib.GetRateLimits(gctx, ctx, gc, true)
	for {
		// lib.Printf("Checking token %d %+v %+v\n", h, rem, wait)
		if rem[h] <= 5 {
			lib.Printf("All GH API tokens are overloaded, maximum points %d, waiting %+v\n", rem[h], wait[h])
			time.Sleep(time.Duration(1) * time.Second)
			time.Sleep(wait[h])
			h, _, rem, wait = lib.GetRateLimits(gctx, ctx, gc, true)
			continue
		}
		if rem[h] >= 500 {
			canCache = true
		}
		break
	}
	aHint = h
	// lib.Printf("Found usable token %d/%d/%v, cache enabled: %v\n", aHint, rem[h], wait[h], canCache)
	return
}

func isAbuse(e error) bool {
	if e == nil {
		return false
	}
	errStr := e.Error()
	return strings.Contains(errStr, "403 You have triggered an abuse detection mechanism") || strings.Contains(errStr, "403 API rate limit")
}

func getGitHubUser(ctx *lib.Ctx, login string) (user map[string]*string, found bool, err error) {
	var (
		ok        bool
		cacheSize int
	)
	// Try memory cache 1st
	if gGitHubUsersMtx != nil {
		gGitHubUsersMtx.RLock()
	}
	user, ok = gGitHubUsers[login]
	if ctx.Debug > 0 {
		cacheSize = len(gGitHubUsers)
	}
	if gGitHubUsersMtx != nil {
		gGitHubUsersMtx.RUnlock()
	}
	if ok {
		found = len(user) > 0
		if ctx.Debug > 0 {
			lib.Printf("getGitHubUser(%d): cache hit: %s\n", cacheSize, login)
		}
		return
	}
	if ctx.Debug > 0 {
		lib.Printf("getGitHubUser(%d): cache miss: %s\n", cacheSize, login)
	}
	// Try file cache 2nd
	path := "cache/" + login + ".json"
	file, e := os.Stat(path)
	if e == nil {
		modified := file.ModTime()
		age := int(time.Now().Sub(modified).Seconds())
		allowedAge := cMaxGitHubUsersFileCacheAge + rand.Intn(cMaxGitHubUsersFileCacheAge)
		if age <= allowedAge {
			bts, e := ioutil.ReadFile(path)
			if e == nil {
				e = jsoniter.Unmarshal(bts, &user)
				bts = nil
				if e == nil {
					found = len(user) > 0
					if ctx.Debug > 0 {
						lib.Printf("getGitHubUser(%d): file cache hit: %s (age: %v/%v)\n", cacheSize, login, time.Duration(age)*time.Second, time.Duration(allowedAge)*time.Second)
					}
					if gGitHubUsersMtx != nil {
						gGitHubUsersMtx.Lock()
					}
					gGitHubUsers[login] = user
					if gGitHubUsersMtx != nil {
						gGitHubUsersMtx.Unlock()
					}
					return
				}
				lib.Printf("getGitHubUser: cannot unmarshal %s cache file: %v\n", path, e)
			} else {
				lib.Printf("getGitHubUser: cannot read %s user cache file: %v\n", path, e)
			}
		} else {
			lib.Printf("getGitHubUser: %s user cache file is too old: %v (allowed %v)\n", path, time.Duration(age)*time.Second, time.Duration(allowedAge)*time.Second)
		}
	} else {
		if ctx.Debug > 0 {
			lib.Printf("getGitHubUser: no %s user cache file: %v\n", path, e)
		}
	}
	defer func() {
		if err != nil {
			return
		}
		path := "cache/" + login + ".json"
		bts, err := jsoniter.Marshal(user)
		if err != nil {
			lib.Printf("getGitHubUser: cannot marshal user %s to file %s\n", login, path)
			return
		}
		err = ioutil.WriteFile(path, bts, 0644)
		if err != nil {
			lib.Printf("getGitHubUser: cannot write file %s, %d bytes\n", path, len(bts))
			return
		}
		if ctx.Debug > 0 {
			lib.Printf("getGitHubUser: saved %s user file\n", path)
		}
	}()
	// Try GitHub API 3rd
	var c *github.Client
	if gGitHubMtx != nil {
		gGitHubMtx.RLock()
	}
	if !gInit || gHint < 0 {
		if ctx.Debug > 0 {
			lib.Printf("getGitHubUser: init\n")
		}
		if gGitHubMtx != nil {
			gGitHubMtx.RUnlock()
			gGitHubMtx.Lock()
		}
		if !gInit {
			gctx, gc = lib.GHClient(ctx)
			gInit = true
		}
		gHint, _ = handleRate(ctx)
		c = gc[gHint]
		if gGitHubMtx != nil {
			gGitHubMtx.Unlock()
		}
	} else {
		c = gc[gHint]
		if gGitHubMtx != nil {
			gGitHubMtx.RUnlock()
		}
		if ctx.Debug > 0 {
			lib.Printf("getGitHubUser: reuse\n")
		}
	}
	retry := false
	for {
		var (
			response *github.Response
			usr      *github.User
			e        error
		)
		if ctx.Debug > 0 {
			lib.Printf("getGitHubUser: ask %s\n", login)
		}
		usr, response, e = c.Users.Get(gctx, login)
		// lib.Printf("GET %s -> {%+v, %+v, %+v}\n", login, usr, response, e)
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if gGitHubUsersMtx != nil {
				gGitHubUsersMtx.Lock()
			}
			gGitHubUsers[login] = map[string]*string{}
			if gGitHubUsersMtx != nil {
				gGitHubUsersMtx.Unlock()
			}
			return
		}
		if e != nil && !retry {
			lib.Printf("Error getting %s user: response: %+v, error: %+v, retrying rate\n", login, response, e)
			lib.Printf("getGitHubUser: handle rate\n")
			abuse := isAbuse(err)
			if abuse {
				sleepFor := 30 + rand.Intn(30)
				lib.Printf("GitHub detected abuse (get user %s), waiting for %ds\n", login, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if gGitHubMtx != nil {
				gGitHubMtx.Lock()
			}
			gHint, _ = handleRate(ctx)
			if gGitHubMtx != nil {
				gGitHubMtx.Unlock()
			}
			if !abuse {
				retry = true
			}
			continue
		}
		if e != nil {
			err = e
			return
		}
		if usr != nil {
			user = map[string]*string{"name": usr.Name, "email": usr.Email, "company": usr.Company, "location": usr.Location}
			found = true
		}
		break
	}
	if gGitHubUsersMtx != nil {
		gGitHubUsersMtx.Lock()
	}
	gGitHubUsers[login] = user
	if gGitHubUsersMtx != nil {
		gGitHubUsersMtx.Unlock()
	}
	return
}

func processEndpoint(ctx *lib.Ctx, ep *lib.RawEndpoint, git bool, key [2]string, orgsMap, reposMap, resMap map[[2]string]map[string]struct{}, allReposMap map[string]struct{}, cache map[string][]string) {
	keyAll := [2]string{"", ""}
	if strings.HasPrefix(ep.Name, `regexp:`) {
		re := ep.Name[7:]
		if strings.HasPrefix(re, "(?i)") {
			re = re[4:]
		}
		if strings.HasPrefix(re, "^") {
			re = re[1:]
		}
		if strings.HasSuffix(re, "$") {
			re = re[:len(re)-1]
		}
		resMap[keyAll][re] = struct{}{}
		_, ok := resMap[key]
		if !ok {
			resMap[key] = make(map[string]struct{})
		}
		resMap[key][re] = struct{}{}
		return
	}
	if git && !strings.Contains(ep.Name, `://github.com/`) {
		return
	}
	if len(ep.Flags) == 0 {
		ary := strings.Split(ep.Name, "/")
		tokens := []string{}
		for _, token := range ary {
			if token != "" {
				tokens = append(tokens, token)
			}
		}
		if len(tokens) < 3 {
			return
		}
		l := len(tokens)
		r := tokens[l-2] + "/" + tokens[l-1]
		reposMap[keyAll][r] = struct{}{}
		_, ok := reposMap[key]
		if !ok {
			reposMap[key] = make(map[string]struct{})
		}
		reposMap[key][r] = struct{}{}
		allReposMap[r] = struct{}{}
		return
	}
	tp, ok := ep.Flags["type"]
	if !ok {
		return
	}
	if tp != lib.GitHubOrg && tp != lib.GitHubUser {
		return
	}
	fetchAllMode := false
	if len(ep.Skip) == 0 && len(ep.Only) == 0 {
		ary := strings.Split(ep.Name, "/")
		tokens := []string{}
		for _, token := range ary {
			if token != "" {
				tokens = append(tokens, token)
			}
		}
		if len(tokens) < 2 {
			return
		}
		l := len(tokens)
		o := tokens[l-1]
		orgsMap[keyAll][o] = struct{}{}
		_, ok := orgsMap[key]
		if !ok {
			orgsMap[key] = make(map[string]struct{})
		}
		orgsMap[key][o] = struct{}{}
		// If we were not to get all retos we could just return here
		// return
		fetchAllMode = true
	}
	arr := strings.Split(ep.Name, "/")
	ary := []string{}
	l := len(arr) - 1
	for i, s := range arr {
		if i == l && s == "" {
			break
		}
		ary = append(ary, s)
	}
	lAry := len(ary)
	path := ary[lAry-1]
	root := strings.Join(ary[0:lAry-1], "/")
	// cacheKey := path + ":" + strings.Join(ep.Skip, ",") + ":" + strings.Join(ep.Only, ",")
	cacheKey := path
	repos, ok := cache[cacheKey]
	if !ok {
		if !gInit {
			gctx, gc = lib.GHClient(ctx)
			gInit = true
		}
		var hint int
		if gHint < 0 {
			var canCache bool
			hint, canCache = handleRate(ctx)
			if canCache {
				gHint = hint
			}
		} else {
			hint = gHint
		}
		// lib.Printf("org: gInit, gHint = %v, %d\n", gInit, gHint)
		var (
			optOrg  *github.RepositoryListByOrgOptions
			optUser *github.RepositoryListOptions
		)
		if tp == lib.GitHubOrg {
			optOrg = &github.RepositoryListByOrgOptions{Type: "public"}
			optOrg.PerPage = 100
		} else {
			optUser = &github.RepositoryListOptions{Type: "public"}
			optUser.PerPage = 100
		}
		retry := false
		for {
			var (
				repositories []*github.Repository
				response     *github.Response
				err          error
			)
			if tp == lib.GitHubOrg {
				repositories, response, err = gc[hint].Repositories.ListByOrg(gctx, path, optOrg)
			} else {
				repositories, response, err = gc[hint].Repositories.List(gctx, path, optUser)
			}
			if err != nil && !retry {
				lib.Printf("Error getting repositories list for org/user: %s: response: %+v, error: %+v, retrying rate\n", path, response, err)
				abuse := isAbuse(err)
				if abuse {
					sleepFor := 30 + rand.Intn(30)
					lib.Printf("GitHub detected abuse (get org/user repos %s), waiting for %ds\n", path, sleepFor)
					time.Sleep(time.Duration(sleepFor) * time.Second)
				}
				hint, _ = handleRate(ctx)
				if !abuse {
					retry = true
				}
				continue
			}
			lib.FatalOnError(err)
			for _, repo := range repositories {
				if repo.Name != nil {
					name := root + "/" + path + "/" + *(repo.Name)
					repos = append(repos, name)
				}
			}
			if response.NextPage == 0 {
				break
			}
			if tp == lib.GitHubOrg {
				optOrg.Page = response.NextPage
			} else {
				optUser.Page = response.NextPage
			}
			retry = false
		}
		cache[cacheKey] = repos
		// lib.Printf("org/user: %s skip=%+v only=%+v -> %+v\n", ep.Name, ep.Skip, ep.Only, repos)
	}
	for _, skip := range ep.Skip {
		skipRE, err := regexp.Compile(skip)
		lib.FatalOnError(err)
		ep.SkipREs = append(ep.SkipREs, skipRE)
	}
	for _, only := range ep.Only {
		onlyRE, err := regexp.Compile(only)
		lib.FatalOnError(err)
		ep.OnlyREs = append(ep.OnlyREs, onlyRE)
	}
	for _, repo := range repos {
		included, _ := lib.EndpointIncluded(ctx, ep, repo)
		if !included {
			continue
		}
		ary := strings.Split(repo, "/")
		tokens := []string{}
		for _, token := range ary {
			if token != "" {
				tokens = append(tokens, token)
			}
		}
		if len(tokens) < 3 {
			return
		}
		l := len(tokens)
		r := tokens[l-2] + "/" + tokens[l-1]
		allReposMap[r] = struct{}{}
		if fetchAllMode {
			continue
		}
		reposMap[keyAll][r] = struct{}{}
		_, ok := reposMap[key]
		if !ok {
			reposMap[key] = make(map[string]struct{})
		}
		reposMap[key][r] = struct{}{}
	}
}

func processFixtures(ctx *lib.Ctx, fixtureFiles []string) (config map[[2]string]*regexp.Regexp, allRepos []string) {
	fixtures := []lib.Fixture{}
	if gThrN > 1 {
		ch := make(chan lib.Fixture)
		nThreads := 0
		for _, fixtureFile := range fixtureFiles {
			if fixtureFile == "" {
				continue
			}
			go processFixtureFile(ch, ctx, fixtureFile)
			nThreads++
			if nThreads == gThrN {
				fixture := <-ch
				nThreads--
				if fixture.Disabled != true {
					fixtures = append(fixtures, fixture)
				}
			}
		}
		for nThreads > 0 {
			fixture := <-ch
			nThreads--
			if fixture.Disabled != true {
				fixtures = append(fixtures, fixture)
			}
		}
	} else {
		for _, fixtureFile := range fixtureFiles {
			if fixtureFile == "" {
				continue
			}
			fixture := processFixtureFile(nil, ctx, fixtureFile)
			if fixture.Disabled != true {
				fixtures = append(fixtures, fixture)
			}
		}
	}
	if len(fixtures) == 0 {
		lib.Fatalf("No fixtures read, this is error, please define at least one")
	}
	keyAll := [2]string{"", ""}
	keys := make(map[[2]string]struct{})
	orgs := make(map[[2]string]map[string]struct{})
	repos := make(map[[2]string]map[string]struct{})
	res := make(map[[2]string]map[string]struct{})
	orgs[keyAll] = make(map[string]struct{})
	repos[keyAll] = make(map[string]struct{})
	res[keyAll] = make(map[string]struct{})
	cache := make(map[string][]string)
	allReposMap := make(map[string]struct{})
	gHint = -1
	for _, fixture := range fixtures {
		fSlug := fixture.Native.Slug
		suff := ""
		cats := make(map[string]struct{})
		for _, ds := range fixture.DataSources {
			dss := strings.ToLower(strings.TrimSpace(ds.Slug))
			ary := strings.Split(dss, "/")
			if ary[0] != "github" {
				continue
			}
			// We merge PR & issue data into te same index
			if ary[1] == "pull_request" {
				ary[1] = "issue"
			}
			_, ok := cats[ary[1]]
			if !ok {
				cats[ary[1]] = struct{}{}
				suff += ary[1] + "=" + ds.IndexSuffix + ","
			}
		}
		if suff != "" {
			suff := suff[:len(suff)-1]
			fSlug += ":" + suff
		}
		for _, ds := range fixture.DataSources {
			include := false
			git := false
			dss := strings.ToLower(strings.TrimSpace(ds.Slug))
			if dss == "git" {
				// include = true
				include = false
				git = true
			} else {
				ary := strings.Split(dss, "/")
				if ary[0] == "github" {
					include = true
				}
			}
			if !include {
				continue
			}
			for _, ep := range ds.RawEndpoints {
				key := [2]string{fSlug, ep.Project}
				processEndpoint(ctx, &ep, git, key, orgs, repos, res, allReposMap, cache)
				keys[key] = struct{}{}
			}
			for _, ep := range ds.HistEndpoints {
				key := [2]string{fSlug, ep.Project}
				processEndpoint(ctx, &ep, git, key, orgs, repos, res, allReposMap, cache)
				keys[key] = struct{}{}
			}
			for _, project := range ds.Projects {
				proj := project.Name
				if proj == "" {
					lib.Fatalf("Empty project name entry in %+v, data source %s, fixture %s\n", project, ds.Slug, fSlug)
				}
				for _, ep := range project.RawEndpoints {
					eProj := proj
					if ep.Project != "" {
						eProj = ep.Project
					}
					key := [2]string{fSlug, eProj}
					processEndpoint(ctx, &ep, git, key, orgs, repos, res, allReposMap, cache)
					keys[key] = struct{}{}
				}
				for _, ep := range project.HistEndpoints {
					eProj := proj
					if ep.Project != "" {
						eProj = ep.Project
					}
					key := [2]string{fSlug, eProj}
					processEndpoint(ctx, &ep, git, key, orgs, repos, res, allReposMap, cache)
					keys[key] = struct{}{}
				}
			}
		}
	}
	keysAry := [][2]string{}
	keysAry = append(keysAry, keyAll)
	for key := range keys {
		keysAry = append(keysAry, key)
	}
	sort.Slice(
		keysAry,
		func(i, j int) bool {
			a := keysAry[i][0] + "," + keysAry[i][1]
			b := keysAry[j][0] + "," + keysAry[j][1]
			return a < b
		},
	)
	for repo := range allReposMap {
		allRepos = append(allRepos, repo)
	}
	sort.Strings(allRepos)
	config = make(map[[2]string]*regexp.Regexp)
	for _, key := range keysAry {
		orgsAry := []string{}
		reposAry := []string{}
		resAry := []string{}
		for org := range orgs[key] {
			orgsAry = append(orgsAry, org)
		}
		for repo := range repos[key] {
			reposAry = append(reposAry, repo)
		}
		for re := range res[key] {
			resAry = append(resAry, re)
		}
		sort.Strings(orgsAry)
		sort.Strings(reposAry)
		sort.Strings(resAry)
		ary := strings.Split(key[0], ":")
		slug := ary[0]
		proj := key[1]
		re := ``
		n := 0
		for _, org := range orgsAry {
			re += org + `\/.*|`
			n++
		}
		for _, repo := range reposAry {
			re += strings.Replace(repo, `/`, `\/`, -1) + `|`
			n++
		}
		for _, r := range resAry {
			re += `(` + r + `)|`
			n++
		}
		if n == 0 {
			// lib.Printf("Slug: '%s', Project: '%s': no data\n", slug, proj)
			continue
		}
		if n == 1 {
			re = `^` + re[0:len(re)-1] + `$`
		} else {
			re = `^(` + re[0:len(re)-1] + `)$`
		}
		cre, err := regexp.Compile(re)
		if err != nil {
			lib.Printf("Failed: Slug: '%s', Project: '%s', RE: %s\n", slug, proj, re)
		}
		lib.FatalOnError(err)
		if ctx.Debug > 0 {
			lib.Printf("Slug: '%s', Project: '%s', RE: %v\n", slug, proj, cre)
		}
		config[key] = cre
	}
	// Eventually save config
	if ctx.SaveConfig {
		lib.FatalOnError(saveConfigFixtures(ctx, config, allRepos))
	}
	return
}

func serializeConfig(config map[[2]string]*regexp.Regexp) (serialized map[string]string) {
	serialized = make(map[string]string)
	for k, v := range config {
		serialized[k[1]+": "+k[0]] = v.String()
	}
	return
}

func deserializeConfig(serialized map[string]string) (config map[[2]string]*regexp.Regexp) {
	config = make(map[[2]string]*regexp.Regexp)
	for k, v := range serialized {
		ary := strings.Split(k, ": ")
		nk := [2]string{ary[1], ary[0]}
		re, err := regexp.Compile(v)
		lib.FatalOnError(err)
		config[nk] = re
	}
	return
}

func saveConfigStartDates(ctx *lib.Ctx, startDates map[string]map[string]time.Time) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "saveConfigStartDates")
		}
	}()
	var bts []byte
	bts, err = jsoniter.Marshal(startDates)
	if err != nil {
		return
	}
	fn := ctx.ConfigFile + "dates.json"
	err = ioutil.WriteFile(fn, bts, 0644)
	return
}

func saveConfigFixtures(ctx *lib.Ctx, config map[[2]string]*regexp.Regexp, allRepos []string) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "saveConfigFixtures")
		}
	}()
	var bts []byte
	bts, err = jsoniter.Marshal(serializeConfig(config))
	if err != nil {
		return
	}
	fn := ctx.ConfigFile + "fixtures.json"
	err = ioutil.WriteFile(fn, bts, 0644)
	bts, err = jsoniter.Marshal(allRepos)
	if err != nil {
		return
	}
	fn = ctx.ConfigFile + "repos.json"
	err = ioutil.WriteFile(fn, bts, 0644)
	return
}

func loadConfigStartDates(ctx *lib.Ctx) (startDates map[string]map[string]time.Time, err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "loadConfigStartDates")
		}
	}()
	var bts []byte
	fn := ctx.ConfigFile + "dates.json"
	bts, err = ioutil.ReadFile(fn)
	if err != nil {
		return
	}
	err = jsoniter.Unmarshal(bts, &startDates)
	return
}

func loadConfigFixtures(ctx *lib.Ctx) (config map[[2]string]*regexp.Regexp, allRepos []string, err error) {
	defer func() {
		if err != nil {
			err = errors.Wrap(err, "loadConfigFixtures")
		}
	}()
	var bts []byte
	fn := ctx.ConfigFile + "fixtures.json"
	bts, err = ioutil.ReadFile(fn)
	if err != nil {
		return
	}
	var data map[string]string
	err = jsoniter.Unmarshal(bts, &data)
	config = deserializeConfig(data)
	fn = ctx.ConfigFile + "repos.json"
	bts, err = ioutil.ReadFile(fn)
	if err != nil {
		return
	}
	err = jsoniter.Unmarshal(bts, &allRepos)
	// lib.Printf("%+v (%d)\n", allRepos, len(allRepos))
	return
}

func repoHit(fullName string, re *regexp.Regexp) bool {
	if fullName == "" {
		return false
	}
	// lib.Printf("repoHit('%s', %v) --> %v\n", fullName, re, re.MatchString(fullName))
	return re.MatchString(fullName)
}

func ensureIndex(ch chan struct{}, ctx *lib.Ctx, idx string) {
	defer func() {
		if ch != nil {
			ch <- struct{}{}
		}
	}()
	method := "HEAD"
	url := ctx.ESURL + "/" + idx
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		if ctx.Debug > 0 {
			lib.Printf("new request error: %+v for %s url: %s\n", err, method, url)
		}
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		if ctx.Debug > 0 {
			lib.Printf("do request error: %+v for %s url: %s\n", err, method, url)
		}
		return
	}
	if resp.StatusCode != 200 {
		if ctx.Debug > 0 {
			lib.Printf("check exists %s --> %d\n", idx, resp.StatusCode)
		}
		method = "PUT"
		req, err := http.NewRequest(method, url, nil)
		if err != nil {
			if ctx.Debug > 0 {
				lib.Printf("new request error: %+v for %s url: %s\n", err, method, url)
			}
			return
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			if ctx.Debug > 0 {
				lib.Printf("do request error: %+v for %s url: %s\n", err, method, url)
			}
			return
		}
		if ctx.Debug > 0 {
			lib.Printf("create %s --> %d\n", idx, resp.StatusCode)
		}
		url += "/_mapping"
		data := gGitHubRichMapping
		payloadBytes := []byte(data)
		payloadBody := bytes.NewReader(payloadBytes)
		req, err = http.NewRequest(method, url, payloadBody)
		if err != nil {
			if ctx.Debug > 0 {
				lib.Printf("new request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
			}
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			if ctx.Debug > 0 {
				lib.Printf("do request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
			}
			return
		}
		if ctx.Debug > 0 {
			lib.Printf("github rich mapping %s --> %d\n", idx, resp.StatusCode)
		}
		payloadBytes = dads.MappingNotAnalyzeString
		data = string(payloadBytes)
		payloadBody = bytes.NewReader(payloadBytes)
		req, err = http.NewRequest(method, url, payloadBody)
		if err != nil {
			if ctx.Debug > 0 {
				lib.Printf("new request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
			}
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			if ctx.Debug > 0 {
				lib.Printf("do request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
			}
			return
		}
		if ctx.Debug > 0 {
			lib.Printf("not analyze string %s --> %d\n", idx, resp.StatusCode)
		}
	}
	// safe set ensured index
	if gEnsuredIndicesMtx != nil {
		gEnsuredIndicesMtx.Lock()
	}
	gEnsuredIndices[idx] = struct{}{}
	if gEnsuredIndicesMtx != nil {
		gEnsuredIndicesMtx.Unlock()
	}
}

// runs inside gRichMtx so accessing gRichItems is safe
func ensureIndicesPresent(ctx *lib.Ctx) {
	neededIndices := make(map[string]struct{})
	for _, riches := range gRichItems {
		for _, rich := range riches {
			neededIndices[rich["index"].(string)] = struct{}{}
		}
	}
	// fmt.Printf("Needed indices: %+v\n", neededIndices)
	if gThrN > 1 {
		nThreads := 0
		ch := make(chan struct{})
		for idx := range neededIndices {
			gEnsuredIndicesMtx.Lock()
			_, ok := gEnsuredIndices[idx]
			gEnsuredIndicesMtx.Unlock()
			if ok {
				continue
			}
			go ensureIndex(ch, ctx, idx)
			nThreads++
			if nThreads == gThrN {
				<-ch
				nThreads--
			}
		}
		for nThreads > 0 {
			<-ch
			nThreads--
		}
	} else {
		for idx := range neededIndices {
			_, ok := gEnsuredIndices[idx]
			if ok {
				continue
			}
			ensureIndex(nil, ctx, idx)
		}
	}
}

func prettyPrint(data interface{}) string {
	j := jsoniter.Config{SortMapKeys: true, EscapeHTML: true}.Froze()
	pretty, err := j.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Sprintf("%T: %+v", data, data)
	}
	return string(pretty)
}

func printObj(data interface{}) string {
	j := jsoniter.Config{SortMapKeys: true, EscapeHTML: true}.Froze()
	pretty, err := j.Marshal(data)
	if err != nil {
		return fmt.Sprintf("%+v", data)
	}
	return string(pretty)
}

func markSyncDates(ctx *lib.Ctx, items []map[string]interface{}) (err error) {
	n, u := 0, 0
	for _, item := range items {
		index, _ := item["index"].(string)
		origin, _ := item["github_repo"].(string)
		//enriched, _ := item["metadata__enriched_on"].(time.Time)
		//enriched = lib.HourStart(enriched)
		enriched, _ := item["gha_hour"].(time.Time)
		_, ok := gSyncDates[index]
		if !ok {
			gSyncDates[index] = map[string]time.Time{}
		}
		dt, ok := gSyncDates[index][origin]
		if !ok {
			gSyncDates[index][origin] = enriched
			n++
			continue
		}
		if enriched.After(dt) {
			gSyncDates[index][origin] = enriched
			u++
		}
	}
	lib.Printf("marked %d sync dates, added %d\n", u, n)
	return
}

func uploadToES(ctx *lib.Ctx, data [][]map[string]interface{}) (err error) {
	insertItems := []map[string]interface{}{}
	for _, items := range data {
		_, ok := items[0]["upsert"]
		if ok {
			continue
		}
		for _, item := range items {
			insertItems = append(insertItems, item)
		}
	}
	ch := make(chan error)
	go func() { _ = insertToES(ch, ctx, insertItems) }()
	defer func() {
		e := <-ch
		if err == nil {
			err = fmt.Errorf("insertToES: %+v", e)
			return
		}
		if e == nil {
			err = fmt.Errorf("uploadToES: %+v", err)
			return
		}
		err = fmt.Errorf("insertToES: %+v, uploadToES: %+v", e, err)
	}()
	uuids := make(map[string]struct{})
	indices := make(map[string]struct{})
	uuidsUpdates := make(map[string][]map[string]interface{})
	uuidsCurrent := make(map[string]map[string]interface{})
	key := "metadata__updated_on"
	for _, items := range data {
		_, ok := items[0]["upsert"]
		if !ok {
			continue
		}
		uuid, _ := items[0]["uuid"].(string)
		idx, _ := items[0]["index"].(string)
		uuids[uuid] = struct{}{}
		indices[idx] = struct{}{}
		sort.Slice(
			items,
			func(i, j int) bool {
				a, _ := items[i][key].(time.Time)
				b, _ := items[j][key].(time.Time)
				return a.Before(b)
			},
		)
		uuidsUpdates[uuid] = items
	}
	nUUIDs := len(uuids)
	packSize := ctx.ESBulkSize
	if packSize > 1000 {
		packSize = 1000
	}
	nPacks := nUUIDs / packSize
	if nUUIDs%packSize != 0 {
		nPacks++
	}
	auuids := []string{}
	for uuid := range uuids {
		auuids = append(auuids, uuid)
	}
	packs := []string{}
	for i := 0; i < nPacks; i++ {
		from := i * packSize
		to := from + packSize
		if to > nUUIDs {
			to = nUUIDs
		}
		s := "["
		for j := from; j < to; j++ {
			s += `"` + auuids[j] + `",`
		}
		if s != "[" {
			s = s[:len(s)-1] + "]"
			packs = append(packs, s)
		}
	}
	pattern := ""
	for idx := range indices {
		pattern += idx + ","
	}
	pattern = pattern[:len(pattern)-1]
	nIndices := len(indices)
	lib.Printf("processing %d uuids from %d indices in %d packs\n", nUUIDs, nIndices, nPacks)
	method := "POST"
	fetchCurrent := func(packNum int) (err error) {
		var (
			url     string
			rurl    string
			scroll  *string
			payload *bytes.Reader
		)
		defer func() {
			if scroll == nil {
				return
			}
			url := ctx.ESURL + "/_search/scroll"
			rurl := "/_search/scroll"
			payloadBytes := []byte(`{"scroll_id":"` + *scroll + `"}`)
			payload := bytes.NewReader(payloadBytes)
			req, err := http.NewRequest("DELETE", url, payload)
			if err != nil {
				lib.Printf("new request error: %+v for DELETE url: %s, payload: %s\n", err, rurl, string(payloadBytes))
				err = nil
				return
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				lib.Printf("do request error: %+v for DELETE url: %s, payload: %s\n", err, rurl, string(payloadBytes))
				err = nil
				return
			}
			status := resp.StatusCode
			if status != 200 {
				lib.Printf("%s: deleting scroll: %s %v status: %d\n", pattern, rurl, string(payloadBytes), resp.StatusCode)
			}
		}()
		allItems := 0
		for {
			if scroll == nil {
				url = ctx.ESURL + "/" + pattern + "/_search?scroll=15m&size=1000"
				rurl = "/" + pattern + "/_search?scroll=15m&size=1000"
				payload = bytes.NewReader([]byte(`{"query":{"terms":{"uuid":` + packs[packNum] + `}}}`))
			} else {
				url = ctx.ESURL + "/_search/scroll"
				rurl = "/_search/scroll"
				payload = bytes.NewReader([]byte(`{"scroll":"15m","scroll_id":"` + *scroll + `"}`))
			}
			var req *http.Request
			req, err = http.NewRequest(method, url, payload)
			if err != nil {
				lib.Printf("new request error: %+v for %s url: %s\n", err, method, rurl)
				return
			}
			req.Header.Set("Content-Type", "application/json")
			var resp *http.Response
			resp, err = http.DefaultClient.Do(req)
			if err != nil {
				lib.Printf("do request error: %+v for %s url: %s\n", err, method, rurl)
				return
			}
			var body []byte
			body, err = ioutil.ReadAll(resp.Body)
			_ = resp.Body.Close()
			status := resp.StatusCode
			//fmt.Printf("status: %d\n", status)
			if status == 404 {
				if scroll != nil && strings.Contains(string(body), "No search context found for id") {
					lib.Printf("%s: scroll %s expired, re-running\n", pattern, *scroll)
					scroll = nil
					err = nil
					continue
				}
				return
			}
			if status == 500 {
				if scroll == nil && status == 500 && strings.Contains(string(body), "Trying to create too many scroll contexts") {
					lib.Printf("%s: trying to create too many scrolls, sleeping for 10s\n", pattern)
					time.Sleep(10)
					continue
				}
				return
			}
			if status != 200 {
				s := ""
				if scroll != nil {
					s = *scroll
				}
				err = fmt.Errorf("%s: scroll %s got %d status", pattern, s, status)
				return
			}
			var result map[string]interface{}
			err = jsoniter.Unmarshal(body, &result)
			if err != nil {
				lib.Printf("unmarshal error: %+v for %s url: %s\n", err, method, rurl)
				return
			}
			sScroll, ok := result["_scroll_id"].(string)
			if !ok {
				err = fmt.Errorf("missing _scroll_id in the response")
				return
			}
			scroll = &sScroll
			items, ok := result["hits"].(map[string]interface{})["hits"].([]interface{})
			if !ok {
				err = fmt.Errorf("Missing hits.hits in the response %+v", result)
				return
			}
			nItems := len(items)
			if nItems == 0 {
				lib.Printf("%s finished %d items\n", pattern, allItems)
				break
			}
			for _, item := range items {
				doc, _ := item.(map[string]interface{})["_source"].(map[string]interface{})
				uuid, _ := doc["uuid"].(string)
				uuidsCurrent[uuid] = doc
				// lib.Printf("%d/%d: %s:%d\n", i, nItems, uuid, len(doc))
			}
			allItems += nItems
			lib.Printf("%s processed %d items (%d so far)\n", pattern, nItems, allItems)
		}
		err = nil
		return
	}
	for i := 0; i < nPacks; i++ {
		e := fetchCurrent(i)
		if e != nil {
			lib.Printf("%s: fetching current data: %+v\n", pattern, e)
		}
	}
	lib.Printf("found %d/%d current uuids values\n", len(uuidsCurrent), nUUIDs)
	upsertItems := []map[string]interface{}{}
	for uuid, updates := range uuidsUpdates {
		data, ok := uuidsCurrent[uuid]
		if ok {
			data = mergeIssuePRData(data, uuidsUpdates[uuid][0])
		} else {
			data = uuidsUpdates[uuid][0]
		}
		for _, update := range updates[1:] {
			data = mergeIssuePRData(data, update)
		}
		upsertItems = append(upsertItems, data)
	}
	fmt.Printf("====>\n%+v\n<====\n", upsertItems)
	return
}

func mergeIssuePRData(a, b map[string]interface{}) (res map[string]interface{}) {
	res = make(map[string]interface{})
	mergeValues := func(k string, va, vb interface{}) (v interface{}) {
		defer func() {
			if strings.Contains(k, "labels") {
				lib.Printf("%s: (%v,%T) <-> (%v,%T) -> (%v,%T)\n", k, printObj(va), va, printObj(vb), vb, printObj(v), v)
			}
		}()
		switch k {
		case "comments", "issue_comments", "pr_comments", "issue_time_to_close_days", "pr_time_to_close_days", "time_to_close_days", "issue_time_open_days", "pr_time_open_days", "time_open_days",
			"title", "title_analyzed", "num_review_comments", "commits", "additions", "deletions", "changed_files", "forks", "code_merge_duration":
			// B unless null
			if vb == nil {
				v = va
			} else {
				v = vb
			}
		case "labels":
			vaa, oka := va.([]interface{})
			vba, okb := vb.([]interface{})
			if okb && len(vba) > 0 {
				v = vb
			} else {
				v = va
			}
			if oka && okb && len(vaa) > 0 && len(vba) > 0 {
				fmt.Printf("LABELS: %s: (%v,%v) <-> (%v,%v) -> %v\n", k, vaa, oka, vba, okb, v)
			}
		case "all_labels":
			vaa, oka := va.([]interface{})
			vba, okb := vb.([]interface{})
			labels := make(map[string]struct{})
			if oka {
				for _, it := range vaa {
					labels[it.(string)] = struct{}{}
				}
			}
			if okb {
				for _, it := range vba {
					labels[it.(string)] = struct{}{}
				}
			}
			ary := []string{}
			for label := range labels {
				ary = append(ary, label)
			}
			v = ary
			if oka && okb && len(vaa) > 0 && len(vba) > 0 {
				fmt.Printf("ALL_LABELS: %s: (%v,%v) <-> (%v,%v) -> %v\n", k, vaa, oka, vba, okb, v)
			}
		default:
			// Always prefer B (newer value), so it can update to null
			v = vb
			// fmt.Printf("Default: %s,%v,%T,%v,%T\n", k, va, va, vb, vb)
		}
		return
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok {
			res[k] = va
			continue
		}
		res[k] = mergeValues(k, va, vb)
	}
	for k, vb := range b {
		va, ok := a[k]
		if !ok {
			res[k] = vb
			continue
		}
		res[k] = mergeValues(k, va, vb)
	}
	return
}

func insertToES(ch chan error, ctx *lib.Ctx, items []map[string]interface{}) (err error) {
	// TODO: connect s3 retry mechanism
	if ch != nil {
		defer func() {
			ch <- err
		}()
	}
	nItems := len(items)
	lib.Printf("bulk uploading %d documents (insert)\n", nItems)
	url := ctx.ESURL + "/_bulk?refresh=wait_for"
	payloads := []byte{}
	newLine := []byte("\n")
	var (
		doc []byte
		hdr []byte
	)
	defer func() { runGC() }()
	for _, item := range items {
		doc, err = jsoniter.Marshal(item)
		if err != nil {
			return
		}
		id, ok := item["uuid"].(string)
		if !ok {
			err = fmt.Errorf("missing 'uuid' property in %+v", prettyPrint(item))
			return
		}
		idx, ok := item["index"].(string)
		if !ok {
			err = fmt.Errorf("missing 'index' property in %+v", prettyPrint(item))
			return
		}
		hdr = []byte(`{"index":{"_index":"` + idx + `","_id":"` + id + "\"}}\n")
		payloads = append(payloads, hdr...)
		payloads = append(payloads, doc...)
		payloads = append(payloads, newLine...)
	}
	payloadBody := bytes.NewReader(payloads)
	method := "POST"
	req, err := http.NewRequest(method, url, payloadBody)
	if err != nil {
		err = fmt.Errorf("new request error: %+v for %s url: %s", err, method, url)
		return
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		err = fmt.Errorf("do request error: %+v for %s url: %s", err, method, url)
		return
	}
	var body []byte
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		err = fmt.Errorf("read all response error: %+v for %s url: %s", err, method, url)
		return
	}
	_ = resp.Body.Close()
	var result map[string]interface{}
	err = jsoniter.Unmarshal(body, &result)
	if err != nil {
		return
	}
	ers, ok := result["errors"].(bool)
	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		ers = true
		lib.Printf("bulk upload status:%d, errors flag %v,%v\n%s\n", resp.StatusCode, ers, ok, prettyPrint(result))
	}
	if !ers {
		gDocsUploaded += int64(nItems)
		lib.Printf("bulk uploaded %d documents\n", nItems)
		e := markSyncDates(ctx, items)
		if e != nil {
			lib.Printf("Error marking sync dates for %d items: %v\n", len(items), e)
		}
		return
	}
	lib.Printf("falling back to one-by-one mode because: %s\n", prettyPrint(result))
	errUUIDs := map[string]interface{}{}
	itms, ok := result["items"].([]interface{})
	if ok {
		if len(itms) > 0 {
			for _, itm := range itms {
				it, ok := itm.(map[string]interface{})
				if !ok {
					continue
				}
				index, ok := it["index"].(map[string]interface{})
				if !ok {
					continue
				}
				fStatus, ok := index["status"].(float64)
				if !ok {
					continue
				}
				iid, ok := index["_id"].(string)
				if !ok {
					continue
				}
				status := int(fStatus)
				if status != 200 && status != 201 {
					errUUIDs[iid] = struct{}{}
				}
			}
		}
	}
	uploaded, notUploaded, gaps := 0, 0, 0
	if len(errUUIDs) > 0 {
		lib.Printf("failed %d/%d documents\n", len(errUUIDs), nItems)
		newItems := []map[string]interface{}{}
		for _, item := range items {
			id, _ := item["uuid"].(string)
			_, ok := errUUIDs[id]
			if !ok {
				uploaded++
				continue
			}
			newItems = append(newItems, item)
		}
		if len(newItems) > 0 {
			items = newItems
		}
	}
	method = "PUT"
	uploadedItems := []map[string]interface{}{}
	for _, item := range items {
		notUploaded++
		doc, _ = jsoniter.Marshal(item)
		id, _ := item["uuid"].(string)
		idx, _ := item["index"].(string)
		url = ctx.ESURL + "/" + idx + "/_doc/" + id
		//lib.Printf("%s:\n%s\n", url, prettyPrint(item))
		payloadBody := bytes.NewReader(doc)
		req, e := http.NewRequest(method, url, payloadBody)
		if e != nil {
			lib.Printf("new request error: %+v for %s url: %s, doc: %s\n", e, method, url, prettyPrint(item))
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		trial := 0
		for trial = 0; trial < 3; trial++ {
			resp, e := http.DefaultClient.Do(req)
			if e != nil {
				lib.Printf("do request error: %+v for %s url: %s, doc: %s\n", e, method, url, prettyPrint(item))
				continue
			}
			if resp.StatusCode != 200 && resp.StatusCode != 201 {
				var body []byte
				body, e = ioutil.ReadAll(resp.Body)
				if e != nil {
					lib.Printf("read all response error: %+v for %s url: %s, doc: %s\n", e, method, url, prettyPrint(item))
					continue
				}
				_ = resp.Body.Close()
				var result map[string]interface{}
				e = jsoniter.Unmarshal(body, &result)
				if e != nil {
					lib.Printf("unmarshal response error: %+v for %s url: %s, doc: %s, body: %s\n", e, method, url, prettyPrint(item), string(body))
					continue
				}
				lib.Printf("upload status %d for %s url: %s, doc: %s, result: %s\n", resp.StatusCode, method, url, prettyPrint(item), prettyPrint(result))
				continue
			}
			break
		}
		if trial >= 3 {
			if ctx.GapURL != "" {
				dataEnc := base64.StdEncoding.EncodeToString(doc)
				lib.Printf("Sending item (%d bytes) to the GAP API\n", len(dataEnc))
				gapBody := map[string]string{"payload": dataEnc}
				bData, err := jsoniter.Marshal(gapBody)
				if err != nil {
					lib.Printf("Cannot marshal GAP body: %v: %v\n", gapBody, err)
					continue
				}
				payloadBody := bytes.NewReader(bData)
				method := "POST"
				url := ctx.GapURL
				req, e := http.NewRequest(method, url, payloadBody)
				if e != nil {
					lib.Printf("new request error: %+v for %s url: %s, body: %s\n", e, method, url, prettyPrint(bData))
					continue
				}
				req.Header.Set("Content-Type", "application/json")
				resp, e := http.DefaultClient.Do(req)
				if e != nil {
					lib.Printf("do request error: %+v for %s url: %s, doc: %s\n", e, method, url, prettyPrint(bData))
					continue
				}
				lib.Printf("Sent item (%d bytes) to the GAP API: status: %d\n", len(dataEnc), resp.StatusCode)
				gaps++
			}
			continue
		}
		uploaded++
		notUploaded--
		if ctx.Debug > 0 {
			lib.Printf("uploaded %d, failed %d, gaps %d, all %d\n", uploaded, notUploaded, gaps, nItems)
		}
		uploadedItems = append(uploadedItems, item)
	}
	lib.Printf("uploaded %d/%d documents, failed %d (in one by one fallback)\n", uploaded, nItems, notUploaded)
	gDocsUploaded += int64(uploaded)
	e := markSyncDates(ctx, uploadedItems)
	if e != nil {
		lib.Printf("Error marking sync dates for %d items (one-by-one): %v\n", len(uploadedItems), e)
	}
	return
}

func uploadRichItems(ctx *lib.Ctx, async bool) {
	ensureIndicesPresent(ctx)
	// Even if there is no data, last async jobs can still be running in the background
	// The final call non-async is always ensuring that ES bulk upload job is finished
	if !async && gThrN > 1 {
		gUploadMtx.Lock()
		gUploadMtx.Unlock()
	}
	nItems := len(gRichItems)
	if nItems == 0 {
		return
	}
	items := [][]map[string]interface{}{}
	for _, item := range gRichItems {
		items = append(items, item)
	}
	if async && gThrN > 1 {
		gUploadMtx.Lock()
		go func() {
			err := uploadToES(ctx, items)
			gUploadMtx.Unlock()
			if err != nil {
				lib.Printf("uploadToES: %+v\n", err)
			}
		}()
	} else {
		if gUploadMtx != nil {
			gUploadMtx.Lock()
		}
		err := uploadToES(ctx, items)
		if gUploadMtx != nil {
			gUploadMtx.Unlock()
		}
		if err != nil {
			lib.Printf("uploadToES: %+v\n", err)
		}
	}
	gRichItems = map[string][]map[string]interface{}{}
}

func addRichItem(ctx *lib.Ctx, rich map[string]interface{}) {
	if gRichMtx != nil {
		gRichMtx.Lock()
		defer func() {
			gRichMtx.Unlock()
		}()
	}
	uuid, _ := rich["uuid"].(string)
	riches, ok := gRichItems[uuid]
	if !ok {
		gRichItems[uuid] = []map[string]interface{}{rich}
	} else {
		riches = append(riches, rich)
		gRichItems[uuid] = riches
	}
	nRichItems := len(gRichItems)
	if nRichItems < ctx.ESBulkSize {
		if ctx.Debug > 2 {
			lib.Printf("Pending items: %d\n", nRichItems)
		}
		return
	}
	uploadRichItems(ctx, true)
}

func uploadToDB(ctx *lib.Ctx, pctx *dads.Ctx, items [][3]string) (e error) {
	if ctx.Debug > 0 {
		lib.Printf("bulk uploading %d identities\n", len(items))
	}
	bulkSize := pctx.DBBulkSize / 6
	var tx *sql.Tx
	e = dads.SetDBSessionOrigin(pctx)
	if e != nil {
		return
	}
	tx, e = pctx.DB.Begin()
	if e != nil {
		return
	}
	defer func() { runGC() }()
	nIdents := len(items)
	source := "github"
	run := func(bulk bool) (retry bool, err error) {
		var (
			er   error
			errs []error
			itx  *sql.Tx
		)
		defer func() {
			if bulk {
				if tx != nil {
					lib.Printf("rolling back %d identities insert\n", nIdents)
					_ = tx.Rollback()
					retry = true
				} else {
					if gIdentityMtx != nil {
						gIdentityMtx.Lock()
					}
					for _, item := range items {
						gUploadedIdentities[item] = struct{}{}
					}
					if gIdentityMtx != nil {
						gIdentityMtx.Unlock()
					}
					if ctx.Debug > 0 {
						lib.Printf("bulk uploaded %d identities\n", len(items))
					}
				}
			} else {
				lib.Printf("falling back to one-by-one mode for %d items\n", nIdents)
				defer func() {
					nErrs := len(errs)
					if nErrs == 0 {
						lib.Printf("one-by-one mode for %d items - all succeeded\n", nIdents)
						return
					}
					s := fmt.Sprintf("%d errors: ", nErrs)
					for _, er := range errs {
						s += er.Error() + ", "
					}
					s = s[:len(s)-2]
					err = fmt.Errorf("%s", s)
					lib.Printf("one-by-one mode for %d items: %d errors\n", nIdents, nErrs)
				}()
			}
		}()
		if ctx.Debug >= 0 {
			lib.Printf("adding %d identities, bulk %v\n", nIdents, bulk)
		}
		if bulk {
			nPacks := nIdents / bulkSize
			if nIdents%bulkSize != 0 {
				nPacks++
			}
			for i := 0; i < nPacks; i++ {
				from := i * bulkSize
				to := from + bulkSize
				if to > nIdents {
					to = nIdents
				}
				queryU := "insert ignore into uidentities(uuid,last_modified) values"
				queryI := "insert ignore into identities(id,source,name,email,username,uuid,last_modified) values"
				queryP := "insert ignore into profiles(uuid,name,email) values"
				argsU := []interface{}{}
				argsI := []interface{}{}
				argsP := []interface{}{}
				if ctx.Debug > 0 {
					lib.Printf("bulk adding idents pack #%d %d-%d (%d/%d)\n", i+1, from, to, to-from, nIdents)
				}
				for j := from; j < to; j++ {
					ident := items[j]
					name := ident[0]
					username := ident[1]
					email := ident[2]
					var (
						pname     *string
						pemail    *string
						pusername *string
						profname  *string
					)
					if name != "none" {
						pname = &name
						profname = &name
					}
					if email != "none" {
						pemail = &email
					}
					if username != "none" {
						pusername = &username
						if profname == nil {
							profname = &username
						}
					}
					if pname == nil && pemail == nil && pusername == nil {
						continue
					}
					// if username matches a real email and there is no email set, assume email=username
					if pemail == nil && pusername != nil && dads.IsValidEmail(username) {
						pemail = &username
						email = username
					}
					// if name matches a real email and there is no email set, assume email=name
					if pemail == nil && pname != nil && dads.IsValidEmail(name) {
						pemail = &name
						email = name
					}
					// uuid(source, email, name, username)
					uuid := dads.UUIDAffs(pctx, source, email, name, username)
					if uuid == "" {
						lib.Printf("error: uploadToDb(bulk): failed to generate uuid for (%s,%s,%s,%s), skipping this one\n", source, email, name, username)
						continue
					}
					queryU += fmt.Sprintf("(?,now()),")
					queryI += fmt.Sprintf("(?,?,?,?,?,?,now()),")
					queryP += fmt.Sprintf("(?,?,?),")
					argsU = append(argsU, uuid)
					argsI = append(argsI, uuid, source, pname, pemail, pusername, uuid)
					argsP = append(argsP, uuid, profname, pemail)
				}
				queryU = queryU[:len(queryU)-1]
				queryI = queryI[:len(queryI)-1]
				queryP = queryP[:len(queryP)-1] // + " on duplicate key update name=values(name),email=values(email),last_modified=now()"
				_, err = dads.ExecSQL(pctx, tx, queryU, argsU...)
				if err != nil {
					return
				}
				_, err = dads.ExecSQL(pctx, tx, queryP, argsP...)
				if err != nil {
					return
				}
				_, err = dads.ExecSQL(pctx, tx, queryI, argsI...)
				if err != nil {
					return
				}
			}
			// Will not commit in dry-run mode, deferred function will rollback - so we can still test any errors
			// but the final commit is replaced with rollback
			err = tx.Commit()
			if err != nil {
				return
			}
			tx = nil
		} else {
			for i := 0; i < nIdents; i++ {
				ident := items[i]
				queryU := "insert ignore into uidentities(uuid,last_modified) values"
				queryI := "insert ignore into identities(id,source,name,email,username,uuid,last_modified) values"
				queryP := "insert ignore into profiles(uuid,name,email) values"
				argsU := []interface{}{}
				argsI := []interface{}{}
				argsP := []interface{}{}
				name := ident[0]
				username := ident[1]
				email := ident[2]
				var (
					pname     *string
					pemail    *string
					pusername *string
					profname  *string
				)
				if name != "none" {
					pname = &name
					profname = &name
				}
				if email != "none" {
					pemail = &email
				}
				if username != "none" {
					pusername = &username
					if profname == nil {
						profname = &username
					}
				}
				if pname == nil && pemail == nil && pusername == nil {
					continue
				}
				// if username matches a real email and there is no email set, assume email=username
				if pemail == nil && pusername != nil && dads.IsValidEmail(username) {
					pemail = &username
					email = username
				}
				// if name matches a real email and there is no email set, assume email=name
				if pemail == nil && pname != nil && dads.IsValidEmail(name) {
					pemail = &name
					email = name
				}
				// uuid(source, email, name, username)
				uuid := dads.UUIDAffs(pctx, source, email, name, username)
				if uuid == "" {
					er := fmt.Errorf("error: uploadToDB: failed to generate uuid for (%s,%s,%s,%s)", source, email, name, username)
					lib.Printf("one-by-one(%d/%d): %v\n", i+1, nIdents, er)
					errs = append(errs, er)
					continue
				}
				queryU += fmt.Sprintf("(?,now())")
				queryI += fmt.Sprintf("(?,?,?,?,?,?,now())")
				queryP += fmt.Sprintf("(?,?,?)")
				argsU = append(argsU, uuid)
				argsI = append(argsI, uuid, source, pname, pemail, pusername, uuid)
				argsP = append(argsP, uuid, profname, pemail)
				itx, err = pctx.DB.Begin()
				if err != nil {
					return
				}
				_, er = dads.ExecSQL(pctx, itx, queryU, argsU...)
				if er != nil {
					_ = itx.Rollback()
					lib.Printf("one-by-one(%d/%d): %s[%+v]: %v\n", i+1, nIdents, queryU, argsU, er)
					errs = append(errs, er)
					continue
				}
				_, er = dads.ExecSQL(pctx, itx, queryP, argsP...)
				if er != nil {
					_ = itx.Rollback()
					lib.Printf("one-by-one(%d/%d): %s[%+v]: %v\n", i+1, nIdents, queryP, argsP, er)
					errs = append(errs, er)
					continue
				}
				_, er = dads.ExecSQL(pctx, itx, queryI, argsI...)
				if er != nil {
					_ = itx.Rollback()
					lib.Printf("one-by-one(%d/%d): %s[%+v]: %v\n", i+1, nIdents, queryI, argsI, er)
					errs = append(errs, er)
					continue
				}
				err = itx.Commit()
				if err != nil {
					return
				}
				if gIdentityMtx != nil {
					gIdentityMtx.Lock()
				}
				gUploadedIdentities[ident] = struct{}{}
				if gIdentityMtx != nil {
					gIdentityMtx.Unlock()
				}
				itx = nil
			}
		}
		return
	}
	var retry bool
	retry, e = run(true)
	if retry {
		_, e = run(false)
	}
	return
}

func uploadIdentities(ctx *lib.Ctx, pctx *dads.Ctx, async bool) {
	// Even if there is no data, last async jobs can still be running in the background
	// The final call non-async is always ensuring that ES bulk upload job is finished
	if !async && gThrN > 1 {
		gUploadDBMtx.Lock()
		gUploadDBMtx.Unlock()
	}
	nItems := len(gIdentities)
	if nItems == 0 {
		return
	}
	items := [][3]string{}
	for item := range gIdentities {
		items = append(items, item)
	}
	if async && gThrN > 1 {
		gUploadDBMtx.Lock()
		go func() {
			err := uploadToDB(ctx, pctx, items)
			gUploadDBMtx.Unlock()
			if err != nil {
				lib.Printf("uploadToDB: %+v\n", err)
			}
		}()
	} else {
		if gUploadDBMtx != nil {
			gUploadDBMtx.Lock()
		}
		err := uploadToDB(ctx, pctx, items)
		if gUploadDBMtx != nil {
			gUploadDBMtx.Unlock()
		}
		if err != nil {
			lib.Printf("uploadToDB: %+v\n", err)
		}
	}
	gIdentities = map[[3]string]struct{}{}
}

func addIdentity(ctx *lib.Ctx, identity [3]string) {
	var pctx *dads.Ctx
	if gIdentityMtx != nil {
		gIdentityMtx.Lock()
		defer func() {
			gIdentityMtx.Unlock()
		}()
	}
	pctx = &gDadsCtx
	_, ok := gUploadedIdentities[identity]
	if ok {
		return
	}
	gIdentities[identity] = struct{}{}
	nIdentities := len(gIdentities)
	if nIdentities < pctx.DBBulkSize {
		if ctx.Debug > 1 {
			lib.Printf("Pending identities: %d\n", nIdentities)
		}
		return
	}
	uploadIdentities(ctx, pctx, true)
}

func getForksStarsCountAPI(ctx *lib.Ctx, ev *lib.Event, origin string) (forks, stars, subs, size, openIssues int, fork, ok bool, err error) {
	var (
		repo      map[string]interface{}
		cacheSize int
		found     bool
	)
	if origin != ev.Repo.Name {
		if ctx.Debug > 1 {
			lib.Printf("getForksStarsCountAPI: origin mismatch %s != %s\n", origin, ev.Repo.Name)
		}
		return
	}
	ary := strings.Split(origin, "/")
	if len(ary) != 2 {
		if ctx.Debug > 1 {
			lib.Printf("getForksStarsCountAPI: incorrect origin %s\n", origin)
		}
		return
	}
	org := ary[0]
	repoName := ary[1]
	setResults := func() {
		if repo == nil || err != nil {
			if ctx.Debug > 1 {
				lib.Printf("getForksStarsCountAPI: setResults failed (%v,%v)\n", repo, err)
			}
			return
		}
		ok = true
		forks, _ = repo["forks"].(int)
		stars, _ = repo["stars"].(int)
		subs, _ = repo["subs"].(int)
		size, _ = repo["size"].(int)
		openIssues, _ = repo["open_issues"].(int)
		fork, _ = repo["fork"].(bool)
		// fmt.Printf("getForksStarsCountAPI: %s -> (%d, %d, %d, %d, %d, %v, %v, %v)\n", origin, forks, stars, subs, size, openIssues, fork, ok, err)
	}
	// Try memory cache 1st
	if gGitHubReposMtx != nil {
		gGitHubReposMtx.RLock()
	}
	repo, found = gGitHubRepos[origin]
	if ctx.Debug > 0 {
		cacheSize = len(gGitHubRepos)
	}
	if gGitHubReposMtx != nil {
		gGitHubReposMtx.RUnlock()
	}
	if found {
		if repo == nil {
			if ctx.Debug > 1 {
				lib.Printf("getForksStarsCountAPI: repo %s found in the cache, but is nil\n", origin)
			}
			return
		}
		age := time.Now().Sub(repo["at"].(time.Time))
		if ctx.Debug > 0 {
			lib.Printf("getForksStarsCountAPI(%d): cache hit: %s (age %v)\n", cacheSize, origin, age)
		}
		if age.Seconds() < cAllowedRepoAPIAge {
			setResults()
			return
		}
		if ctx.Debug > 0 {
			lib.Printf("getForksStarsCountAPI(%d): cache expired: %s (age %v)\n", cacheSize, origin, age)
		}
		found = false
	} else {
		if ctx.Debug > 0 {
			lib.Printf("getForksStarsCountAPI(%d): cache miss: %s\n", cacheSize, origin)
		}
	}
	// Try GitHub API 2nd
	var c *github.Client
	if gGitHubMtx != nil {
		gGitHubMtx.RLock()
	}
	if !gInit || gHint < 0 {
		if ctx.Debug > 0 {
			lib.Printf("getForksStarsCountAPI: init\n")
		}
		if gGitHubMtx != nil {
			gGitHubMtx.RUnlock()
			gGitHubMtx.Lock()
		}
		if !gInit {
			gctx, gc = lib.GHClient(ctx)
			gInit = true
		}
		gHint, _ = handleRate(ctx)
		c = gc[gHint]
		if gGitHubMtx != nil {
			gGitHubMtx.Unlock()
		}
	} else {
		c = gc[gHint]
		if gGitHubMtx != nil {
			gGitHubMtx.RUnlock()
		}
		if ctx.Debug > 0 {
			lib.Printf("getForksStarsCountAPI: reuse\n")
		}
	}
	retry := false
	for {
		var (
			response *github.Response
			rep      *github.Repository
			e        error
		)
		if ctx.Debug > 0 {
			lib.Printf("getForksStarsCountAPI: ask %s\n", origin)
		}
		rep, response, e = c.Repositories.Get(gctx, org, repoName)
		// lib.Printf("GET %s/%s -> {%+v, %+v, %+v}\n", org, repoName, rep, response, e)
		if e != nil && strings.Contains(e.Error(), "404 Not Found") {
			if gGitHubReposMtx != nil {
				gGitHubReposMtx.Lock()
			}
			gGitHubRepos[origin] = nil
			if gGitHubReposMtx != nil {
				gGitHubReposMtx.Unlock()
			}
			if ctx.Debug > 1 {
				lib.Printf("getForksStarsCountAPI: repo not found %s: %v\n", origin, err)
			}
			return
		}
		if e != nil && !retry {
			lib.Printf("Error getting %s repo: response: %+v, error: %+v, retrying rate\n", origin, response, e)
			lib.Printf("getForksStarsCountAPI: handle rate\n")
			abuse := isAbuse(err)
			if abuse {
				sleepFor := 30 + rand.Intn(30)
				lib.Printf("GitHub detected abuse (get repo %s), waiting for %ds\n", origin, sleepFor)
				time.Sleep(time.Duration(sleepFor) * time.Second)
			}
			if gGitHubMtx != nil {
				gGitHubMtx.Lock()
			}
			gHint, _ = handleRate(ctx)
			if gGitHubMtx != nil {
				gGitHubMtx.Unlock()
			}
			if !abuse {
				retry = true
			}
			continue
		}
		if e != nil {
			err = e
			return
		}
		if rep != nil {
			if rep.ForksCount != nil {
				forks = *rep.ForksCount
			}
			if rep.StargazersCount != nil {
				stars = *rep.StargazersCount
			}
			if rep.SubscribersCount != nil {
				subs = *rep.SubscribersCount
			}
			if rep.Size != nil {
				size = *rep.Size
			}
			if rep.OpenIssuesCount != nil {
				openIssues = *rep.OpenIssuesCount
			}
			if rep.Fork != nil {
				fork = *rep.Fork
			}
			repo = map[string]interface{}{
				"forks":       forks,
				"stars":       stars,
				"subs":        subs,
				"size":        size,
				"open_issues": openIssues,
				"fork":        fork,
				"at":          time.Now(),
			}
			if ctx.Debug > 1 {
				lib.Printf("getForksStarsCountAPI: found repo: %s: %v\n", origin, repo)
			}
			setResults()
		}
		break
	}
	if gGitHubReposMtx != nil {
		gGitHubReposMtx.Lock()
	}
	gGitHubRepos[origin] = repo
	if gGitHubReposMtx != nil {
		gGitHubReposMtx.Unlock()
	}
	return
}

func getForksStarsCount(ctx *lib.Ctx, ev *lib.Event, origin string) (forks, stars, size, openIssues int, fork, ok bool) {
	if ev.Payload.PullRequest == nil {
		return
	}
	pr := *ev.Payload.PullRequest
	if pr.Base.Repo == nil {
		if ctx.Debug > 1 {
			lib.Printf("no pr.base.repo specified in the event type %s\n", ev.Type)
		}
		return
	}
	repo := *pr.Base.Repo
	if origin != repo.FullName {
		if ctx.Debug > 1 {
			lib.Printf("no pr.base.repo.fullname %s not matching origin %s the event type %s\n", repo.FullName, origin, ev.Type)
		}
		return
	}
	forks = repo.Forks
	stars = repo.StargazersCount
	size = repo.Size
	openIssues = repo.OpenIssues
	fork = repo.Fork
	ok = true
	return
}

func getForksStarsCountOld(ctx *lib.Ctx, ev *lib.EventOld, origin string) (forks, stars, size, openIssues int, fork, ok bool) {
	repo := ev.Repository
	repoName := ""
	if repo.Organization == nil || *repo.Organization == "" {
		repoName = repo.Name
	} else {
		repoName = *repo.Organization + "/" + repo.Name
	}
	if repoName != origin {
		if ctx.Debug > 1 {
			lib.Printf("get forks old: origin mismatch %s != %s\n", repoName, origin)
		}
		return
	}
	forks = repo.Forks
	stars = repo.Stargazers
	size = repo.Size
	openIssues = repo.OpenIssues
	fork = repo.Fork
	ok = true
	return
}

func enrichIssueData(ctx *lib.Ctx, ev *lib.Event, origin string, startDates map[string]map[string]time.Time) (processed bool) {
	//  Possible Issue event types:
	//  IssueCommentEvent
	//  IssuesEvent
	if ev.Payload.Issue == nil {
		lib.Printf("Missing Issue object in %+v\n", prettyPrint(ev))
		return
	}
	issue := ev.Payload.Issue
	fSlug := ev.GHAFxSlug
	suff, ok := ev.GHASuffMap["issue"]
	if !ok {
		if ctx.Debug > 0 {
			lib.Printf("%s: issue not configured\n", origin)
		}
		return
	}
	idx := cPrefix + strings.Replace(fSlug, "/", "-", -1) + "-github-issue" + suff
	var startDate time.Time
	indexStartDates, ok := startDates[idx]
	if ok {
		startDate, ok = indexStartDates[origin]
	}
	hourCreated := lib.HourStart(ev.CreatedAt)
	if ok && startDate.After(hourCreated) {
		if ctx.Debug > 0 {
			lib.Printf("enrichIssueData: %s: %v is after %v, skipping\n", origin, startDate, hourCreated)
		}
		return
	}
	rich := make(map[string]interface{})
	isPullRequest := issue.PullRequest != nil
	itemType := "issue"
	if isPullRequest {
		itemType = "pull request"
	}
	issueID := strconv.Itoa(issue.ID)
	issueNumber := strconv.Itoa(issue.Number)
	now := time.Now()
	repo := "https://github.com/" + origin
	githubRepo := origin
	if strings.HasSuffix(githubRepo, ".git") {
		githubRepo = githubRepo[:len(githubRepo)-4]
	}
	// uuid := dads.UUIDNonEmpty(&dads.Ctx{}, repo, issueID)
	uuid := dads.UUIDNonEmpty(&dads.Ctx{}, githubRepo, issueNumber)
	if uuid == "" {
		lib.Printf("error: enrichIssueData: failed to generate uuid for (%s,%s)\n", repo, issueID)
		return
	}
	rich["upsert"] = true
	rich["event_type"] = ev.Type
	rich["slug"] = ev.GHAFxSlug
	rich["index"] = idx
	rich["project"] = ev.GHAProj
	rich["project_ts"] = time.Now().Unix()
	rich["gha_hour"] = ev.GHADt
	rich["origin"] = repo
	rich["tag"] = repo
	rich["repository"] = repo
	rich["metadata__updated_on"] = ev.CreatedAt
	rich["metadata__timestamp"] = now
	rich["issue_creation_date"] = issue.CreatedAt
	rich["uuid"] = uuid
	rich["issue_id"] = issueID
	rich["is_github_issue"] = 1
	rich["pull_request"] = isPullRequest
	rich["item_type"] = itemType
	rich["metadata__enriched_on"] = now
	rich["offset"] = nil
	rich["github_repo"] = githubRepo
	rich["old_fmt"] = false
	if issue.ClosedAt == nil {
		rich["time_to_close_days"] = nil
	} else {
		rich["time_to_close_days"] = float64(issue.ClosedAt.Sub(issue.CreatedAt).Seconds()) / 86400.0
	}
	rich["issue_time_to_close_days"] = rich["time_to_close_days"]
	if issue.State != "closed" {
		rich["time_open_days"] = float64(now.Sub(issue.CreatedAt).Seconds()) / 86400.0
	} else {
		rich["time_open_days"] = rich["time_to_close_days"]
	}
	rich["issue_time_open_days"] = rich["time_open_days"]
	login := issue.User.Login
	rich["user_login"] = login
	userData, found, err := getGitHubUser(ctx, login)
	if err != nil {
		lib.Printf("Cannot get %s user info: %+v while processing %+v\n", login, err, prettyPrint(ev))
		return
	}
	// name, username, email
	identity := [3]string{"none", login, "none"}
	identities := []map[string]interface{}{}
	roles := []string{}
	if found {
		name := userData["name"]
		email := userData["email"]
		if name != nil {
			identity[0] = *name
		}
		rich["user_name"] = name
		rich["author_name"] = name
		if email != nil {
			identity[2] = *email
			ary := strings.Split(*email, "@")
			if len(ary) > 1 {
				rich["user_domain"] = ary[1]
			}
		} else {
			rich["user_domain"] = nil
		}
		rich["user_org"] = userData["company"]
		rich["user_location"] = userData["location"]
		rich["user_geolocation"] = nil
		addIdentity(ctx, identity)
		identities = append(identities, map[string]interface{}{"name": identity[0], "username": identity[1], "email": identity[2]})
		roles = append(roles, "user_data")
	} else {
		if ctx.Debug > 0 {
			lib.Printf("warning: issue user %s not found\n", login)
		}
		rich["user_name"] = nil
		rich["author_name"] = nil
		rich["user_domain"] = nil
		rich["user_org"] = nil
		rich["user_location"] = nil
		rich["user_geolocation"] = nil
	}
	foundAssignee := false
	if issue.Assignee != nil {
		login := issue.Assignee.Login
		rich["assignee_login"] = login
		userData, found, err := getGitHubUser(ctx, login)
		if err != nil {
			lib.Printf("Cannot get %s assignee info: %+v while processing %+v\n", login, err, prettyPrint(ev))
			return
		}
		if found {
			// name, username, email
			identity := [3]string{"none", login, "none"}
			name := userData["name"]
			email := userData["email"]
			rich["assignee_name"] = name
			if name != nil {
				identity[0] = *name
			}
			if email != nil {
				identity[2] = *email
				ary := strings.Split(*email, "@")
				if len(ary) > 1 {
					rich["assignee_domain"] = ary[1]
				}
			} else {
				rich["assignee_domain"] = nil
			}
			rich["assignee_org"] = userData["company"]
			rich["assignee_location"] = userData["location"]
			rich["assignee_geolocation"] = nil
			addIdentity(ctx, identity)
			identities = append(identities, map[string]interface{}{"name": identity[0], "username": identity[1], "email": identity[2]})
			roles = append(roles, "assignee_data")
			foundAssignee = true
		} else if ctx.Debug > 0 {
			lib.Printf("warning: issue assignee %s not found\n", login)
		}
	}
	if !foundAssignee {
		rich["assignee_name"] = nil
		rich["assignee_domain"] = nil
		rich["assignee_org"] = nil
		rich["assignee_location"] = nil
		rich["assignee_geolocation"] = nil
		rich["assignee_login"] = nil
		rich["assignee_data_uuid"] = ""
		rich["assignee_data_user_name"] = ""
		rich["assignee_data_org_name"] = "Unknown"
		rich["assignee_data_name"] = ""
		rich["assignee_data_multi_org_names"] = []string{"Unknown"}
		rich["assignee_data_id"] = ""
		rich["assignee_data_gender_acc"] = nil
		rich["assignee_data_gender"] = ""
		rich["assignee_data_domain"] = ""
		rich["assignee_data_bot"] = false
	}
	assignees := []string{}
	for i, usr := range issue.Assignees {
		login := usr.Login
		role := "assignees_data:" + strconv.Itoa(i) + ":assignee"
		rich[role+"_login"] = login
		userData, found, err := getGitHubUser(ctx, login)
		if err != nil {
			lib.Printf("Cannot get %s %s info: %+v while processing %+v\n", login, role, err, prettyPrint(ev))
			return
		}
		if found {
			// name, username, email
			identity := [3]string{"none", login, "none"}
			name := userData["name"]
			email := userData["email"]
			rich[role+"_name"] = name
			if name != nil {
				identity[0] = *name
			}
			if email != nil {
				identity[2] = *email
				ary := strings.Split(*email, "@")
				if len(ary) > 1 {
					rich[role+"_domain"] = ary[1]
				}
			} else {
				rich[role+"_domain"] = nil
			}
			rich[role+"_org"] = userData["company"]
			rich[role+"_location"] = userData["location"]
			rich[role+"_geolocation"] = nil
			addIdentity(ctx, identity)
			identities = append(identities, map[string]interface{}{"name": identity[0], "username": identity[1], "email": identity[2]})
			roles = append(roles, role)
			assignees = append(assignees, login)
		} else if ctx.Debug > 0 {
			lib.Printf("warning: PR %s user %s not found\n", role, login)
			rich[role+"_name"] = nil
			rich[role+"_domain"] = nil
			rich[role+"_org"] = nil
			rich[role+"_location"] = nil
			rich[role+"_geolocation"] = nil
			rich[role+"_login"] = nil
			rich[role+"_data_uuid"] = ""
			rich[role+"_data_user_name"] = ""
			rich[role+"_data_org_name"] = "Unknown"
			rich[role+"_data_name"] = ""
			rich[role+"_data_multi_org_names"] = []string{"Unknown"}
			rich[role+"_data_id"] = ""
			rich[role+"_data_gender_acc"] = nil
			rich[role+"_data_gender"] = ""
			rich[role+"_data_domain"] = ""
			rich[role+"_data_bot"] = false
		}
	}
	rich["assignees"] = assignees
	rich["id_in_repo"] = issue.Number
	rich["title"] = issue.Title
	rich["title_analyzed"] = issue.Title
	rich["state"] = issue.State
	rich["issue_state"] = issue.State
	rich["issue_created_at"] = issue.CreatedAt
	rich["created_at"] = issue.CreatedAt
	rich["closed_at"] = issue.ClosedAt
	rich["updated_at"] = issue.UpdatedAt
	rich["issue_closed_at"] = issue.ClosedAt
	rich["issue_updated_at"] = issue.UpdatedAt
	sNumber := strconv.Itoa(issue.Number)
	mid := "/issues/"
	if isPullRequest {
		mid = "/pull/"
	}
	rich["url"] = repo + mid + sNumber
	rich["url_id"] = githubRepo + mid + sNumber
	rich["issue_url"] = rich["url"]
	rich["issue_url_id"] = rich["url_id"]
	labels := []string{}
	for _, label := range issue.Labels {
		labels = append(labels, label.Name)
	}
	rich["all_labels"] = labels
	rich["labels"] = labels
	rich["comments"] = issue.Comments
	rich["issue_comments"] = issue.Comments
	rich["locked"] = issue.Locked
	rich["issue_locked"] = issue.Locked
	if issue.Milestone != nil {
		rich["milestone"] = issue.Milestone.Name
	} else {
		rich["milestone"] = nil
	}
	rich["issue_milestone"] = rich["milestone"]
	// FIXME: we don't have this information in GHA
	// rich["time_to_first_attention"]
	// Affiliations
	if len(identities) > 0 {
		debugSQL := 0
		if ctx.Debug > 0 {
			debugSQL = 2
		}
		pctx := &dads.Ctx{ProjectSlug: ev.GHAFxSlug, DB: gDadsCtx.DB, AffiliationAPIURL: gDadsCtx.AffiliationAPIURL, Debug: ctx.Debug, DebugSQL: debugSQL}
		dt := ev.CreatedAt
		authorKey := "user_data"
		affsItems := make(map[string]interface{})
		var (
			affsIdentity map[string]interface{}
			empty        bool
			err          error
			t            int
			got          bool
		)
		for i, identity := range identities {
			role := roles[i]
			for {
				affsIdentity, empty, err = dads.IdentityAffsData(pctx, gGitHubDS, identity, nil, dt, role)
				if err != nil {
					if t < 3 {
						t++
						lib.Printf("cannot get affiliations data: %v for %v,%v,%s,%s, retrying after %ds\n", err, identity, dt, role, ev.GHAFxSlug, t)
						time.Sleep(time.Duration(t) * time.Second)
						continue
					}
					lib.Printf("cannot get affiliations data: %v for %v,%v,%s,%s, giving up\n", err, identity, dt, role, ev.GHAFxSlug)
					break
				}
				got = true
				break
			}
			if !got {
				return
			}
			if empty {
				email, _ := identity["email"].(string)
				name, _ := identity["name"].(string)
				username, _ := identity["username"].(string)
				id := dads.UUIDAffs(pctx, "github", email, name, username)
				if id == "" {
					lib.Printf("error: enrichIssueData: failed to generate uuid for role %s (%s,github,%s,%s)\n", role, email, name, username)
				}
				if ctx.Debug > 0 {
					lib.Printf("no issue identity affiliation data for %s identity %+v -> pending %s\n", role, identity, id)
				}
				if name == "none" {
					name = ""
				}
				if username == "none" {
					username = ""
				}
				affsIdentity[role+"_id"] = id
				affsIdentity[role+"_uuid"] = id
				affsIdentity[role+"_name"] = name
				affsIdentity[role+"_user_name"] = username
				affsIdentity[role+"_domain"] = dads.IdentityAffsDomain(identity)
				affsIdentity[role+"_gender"] = dads.Unknown
				affsIdentity[role+"_gender_acc"] = nil
				affsIdentity[role+"_org_name"] = dads.Unknown
				affsIdentity[role+"_bot"] = false
				affsIdentity[role+dads.MultiOrgNames] = []interface{}{dads.Unknown}
			}
			for prop, value := range affsIdentity {
				affsItems[prop] = value
			}
			for _, suff := range dads.RequiredAffsFields {
				k := role + suff
				_, ok := affsIdentity[k]
				if !ok {
					affsIdentity[k] = dads.Unknown
				}
			}
		}
		for prop, value := range affsItems {
			setAffsObjectProp(rich, prop, value)
		}
		ks := make(map[string]struct{})
		for k := range rich {
			if strings.HasPrefix(k, "assignees_data:") {
				ks[k] = struct{}{}
			}
		}
		for k := range ks {
			delete(rich, k)
		}
		for _, suff := range dads.AffsFields {
			rich["author"+suff] = rich[authorKey+suff]
		}
		orgsKey := authorKey + dads.MultiOrgNames
		_, ok = rich[orgsKey]
		if !ok {
			rich[orgsKey] = []interface{}{}
		}
		rich["author"+dads.MultiOrgNames] = rich[orgsKey]
	}
	objProps := []string{"assignees_data"}
	for _, objProp := range objProps {
		_, ok := rich[objProp]
		if !ok {
			rich[objProp] = []interface{}{}
		}
		rich["all_"+objProp] = rich[objProp]
	}
	addRichItem(ctx, rich)
	processed = true
	return
}

func enrichPRData(ctx *lib.Ctx, ev *lib.Event, evo *lib.EventOld, origin string, startDates map[string]map[string]time.Time) (processed bool) {
	//  Possible PR event types:
	//  PullRequestEvent
	//  PullRequestReviewCommentEvent
	//  PullRequestReviewEvent
	oldFmt := false
	if ev == nil {
		oldFmt = true
		ev = &lib.Event{
			Payload: lib.Payload{
				PullRequest: evo.Payload.PullRequest,
				Comment:     evo.Payload.Comment,
			},
			GHAFxSlug:  evo.GHAFxSlug,
			GHASuffMap: evo.GHASuffMap,
			GHAProj:    evo.GHAProj,
			GHADt:      evo.GHADt,
			CreatedAt:  evo.CreatedAt,
			Type:       evo.Type,
		}
	}
	if ev.Payload.PullRequest == nil {
		if oldFmt {
			lib.Printf("Missing PR object (old format) in %+v (copied from %v)\n", prettyPrint(ev), prettyPrint(evo))
		} else {
			lib.Printf("Missing PR object in %+v\n", prettyPrint(ev))
		}
		return
	}
	fSlug := ev.GHAFxSlug
	// suff, ok := ev.GHASuffMap["pull_request"]
	suff, ok := ev.GHASuffMap["issue"]
	if !ok {
		if ctx.Debug > 0 {
			lib.Printf("%s: pull request not configured\n", origin)
		}
		return
	}
	// idx := cPrefix + strings.Replace(fSlug, "/", "-", -1) + "-github-pull_request" + suff
	idx := cPrefix + strings.Replace(fSlug, "/", "-", -1) + "-github-issue" + suff
	var startDate time.Time
	indexStartDates, ok := startDates[idx]
	if ok {
		startDate, ok = indexStartDates[origin]
	}
	hourCreated := lib.HourStart(ev.CreatedAt)
	if ok && startDate.After(hourCreated) {
		if ctx.Debug > 0 {
			lib.Printf("enrichPRData: %s: %v is after %v, skipping\n", origin, startDate, hourCreated)
		}
		return
	}
	rich := make(map[string]interface{})
	now := time.Now()
	pr := ev.Payload.PullRequest
	prID := strconv.Itoa(pr.ID)
	prNumber := strconv.Itoa(pr.Number)
	repo := "https://github.com/" + origin
	githubRepo := origin
	if strings.HasSuffix(githubRepo, ".git") {
		githubRepo = githubRepo[:len(githubRepo)-4]
	}
	rich["github_repo"] = githubRepo
	// uuid := dads.UUIDNonEmpty(&dads.Ctx{}, repo, prID)
	uuid := dads.UUIDNonEmpty(&dads.Ctx{}, githubRepo, prNumber)
	if uuid == "" {
		lib.Printf("error: enrichPRData: failed to generate uuid for (%s,%s)\n", repo, prID)
		return
	}
	rich["upsert"] = true
	rich["event_type"] = ev.Type
	rich["slug"] = ev.GHAFxSlug
	rich["index"] = idx
	rich["project"] = ev.GHAProj
	rich["project_ts"] = time.Now().Unix()
	rich["gha_hour"] = ev.GHADt
	rich["origin"] = repo
	rich["tag"] = repo
	rich["repository"] = repo
	rich["metadata__updated_on"] = ev.CreatedAt
	rich["pr_creation_date"] = pr.CreatedAt
	rich["uuid"] = uuid
	rich["pr_id"] = pr.ID
	rich["is_github_pull_request"] = 1
	rich["pull_request"] = true
	rich["item_type"] = "pull request"
	rich["metadata__enriched_on"] = now
	rich["metadata__timestamp"] = now
	rich["offset"] = nil
	rich["old_fmt"] = oldFmt
	if pr.ClosedAt == nil {
		rich["time_to_close_days"] = nil
	} else {
		rich["time_to_close_days"] = float64(pr.ClosedAt.Sub(pr.CreatedAt).Seconds()) / 86400.0
	}
	rich["pr_issue_time_to_close_days"] = rich["time_to_close_days"]
	if pr.State != "closed" {
		rich["time_open_days"] = float64(now.Sub(pr.CreatedAt).Seconds()) / 86400.0
	} else {
		rich["time_open_days"] = rich["time_to_close_days"]
	}
	rich["pr_time_open_days"] = rich["time_open_days"]
	login := pr.User.Login
	rich["user_login"] = login
	userData, found, err := getGitHubUser(ctx, login)
	if err != nil {
		lib.Printf("Cannot get %s user info: %+v while processing %+v\n", login, err, prettyPrint(ev))
		return
	}
	// name, username, email
	identity := [3]string{"none", login, "none"}
	identities := []map[string]interface{}{}
	roles := []string{}
	if found {
		name := userData["name"]
		email := userData["email"]
		if name != nil {
			identity[0] = *name
		}
		rich["user_name"] = name
		rich["author_name"] = name
		if email != nil {
			identity[2] = *email
			ary := strings.Split(*email, "@")
			if len(ary) > 1 {
				rich["user_domain"] = ary[1]
			}
		} else {
			rich["user_domain"] = nil
		}
		rich["user_org"] = userData["company"]
		rich["user_location"] = userData["location"]
		rich["user_geolocation"] = nil
		addIdentity(ctx, identity)
		identities = append(identities, map[string]interface{}{"name": identity[0], "username": identity[1], "email": identity[2]})
		roles = append(roles, "user_data")
	} else {
		if ctx.Debug > 0 {
			lib.Printf("warning: PR user %s not found\n", login)
		}
		rich["user_name"] = nil
		rich["author_name"] = nil
		rich["user_domain"] = nil
		rich["user_org"] = nil
		rich["user_location"] = nil
		rich["user_geolocation"] = nil
	}
	foundMergedBy := false
	if pr.MergedBy != nil {
		login := pr.MergedBy.Login
		rich["merge_author_login"] = login
		userData, found, err := getGitHubUser(ctx, login)
		if err != nil {
			lib.Printf("Cannot get %s merged_by info: %+v while processing %+v\n", login, err, prettyPrint(ev))
			return
		}
		if found {
			// name, username, email
			identity := [3]string{"none", login, "none"}
			name := userData["name"]
			email := userData["email"]
			rich["merge_author_name"] = name
			if name != nil {
				identity[0] = *name
			}
			if email != nil {
				identity[2] = *email
				ary := strings.Split(*email, "@")
				if len(ary) > 1 {
					rich["merge_author_domain"] = ary[1]
				}
			} else {
				rich["merge_author_domain"] = nil
			}
			rich["merge_author_org"] = userData["company"]
			rich["merge_author_location"] = userData["location"]
			rich["merge_author_geolocation"] = nil
			addIdentity(ctx, identity)
			identities = append(identities, map[string]interface{}{"name": identity[0], "username": identity[1], "email": identity[2]})
			roles = append(roles, "merged_by_data")
			foundMergedBy = true
		} else if ctx.Debug > 0 {
			lib.Printf("warning: PR merged_by %s not found\n", login)
		}
	}
	if !foundMergedBy {
		rich["merge_author_name"] = nil
		rich["merge_author_domain"] = nil
		rich["merge_author_org"] = nil
		rich["merge_author_location"] = nil
		rich["merge_author_geolocation"] = nil
		rich["merge_author_login"] = nil
		rich["merged_by_data_uuid"] = ""
		rich["merged_by_data_user_name"] = ""
		rich["merged_by_data_org_name"] = "Unknown"
		rich["merged_by_data_name"] = ""
		rich["merged_by_data_multi_org_names"] = []string{"Unknown"}
		rich["merged_by_data_id"] = ""
		rich["merged_by_data_gender_acc"] = nil
		rich["merged_by_data_gender"] = ""
		rich["merged_by_data_domain"] = ""
		rich["merged_by_data_bot"] = false
	}
	requestedReviewers := []string{}
	assignees := []string{}
	ptrs := []*[]lib.Actor{pr.RequestedReviewers, pr.Assignees}
	arys := [][]string{requestedReviewers, assignees}
	rols := [][2]string{{"requested_reviewers_data", "requested_reviewer"}, {"assignees_data", "assignee"}}
	for oi := 0; oi < 2; oi++ {
		if ptrs[oi] != nil {
			for i, usr := range *ptrs[oi] {
				login := usr.Login
				role := rols[oi][0] + ":" + strconv.Itoa(i) + ":" + rols[oi][1]
				rich[role+"_login"] = login
				userData, found, err := getGitHubUser(ctx, login)
				if err != nil {
					lib.Printf("Cannot get %s %s info: %+v while processing %+v\n", login, role, err, prettyPrint(ev))
					return
				}
				if found {
					// name, username, email
					identity := [3]string{"none", login, "none"}
					name := userData["name"]
					email := userData["email"]
					rich[role+"_name"] = name
					if name != nil {
						identity[0] = *name
					}
					if email != nil {
						identity[2] = *email
						ary := strings.Split(*email, "@")
						if len(ary) > 1 {
							rich[role+"_domain"] = ary[1]
						}
					} else {
						rich[role+"_domain"] = nil
					}
					rich[role+"_org"] = userData["company"]
					rich[role+"_location"] = userData["location"]
					rich[role+"_geolocation"] = nil
					addIdentity(ctx, identity)
					identities = append(identities, map[string]interface{}{"name": identity[0], "username": identity[1], "email": identity[2]})
					roles = append(roles, role)
					arys[oi] = append(arys[oi], login)
				} else if ctx.Debug > 0 {
					lib.Printf("warning: PR %s user %s not found\n", role, login)
					rich[role+"_name"] = nil
					rich[role+"_domain"] = nil
					rich[role+"_org"] = nil
					rich[role+"_location"] = nil
					rich[role+"_geolocation"] = nil
					rich[role+"_login"] = nil
					rich[role+"_data_uuid"] = ""
					rich[role+"_data_user_name"] = ""
					rich[role+"_data_org_name"] = "Unknown"
					rich[role+"_data_name"] = ""
					rich[role+"_data_multi_org_names"] = []string{"Unknown"}
					rich[role+"_data_id"] = ""
					rich[role+"_data_gender_acc"] = nil
					rich[role+"_data_gender"] = ""
					rich[role+"_data_domain"] = ""
					rich[role+"_data_bot"] = false
				}
			}
		}
	}
	rich["requested_reviewers"] = arys[0]
	rich["assignees"] = arys[1]
	// ReviewComments support
	reviewers := []string{}
	comment := ev.Payload.Comment
	hasReview := false
	if comment != nil && (comment.CommitID != nil || comment.PullRequestReviewID != nil) {
		login := comment.User.Login
		role := "reviewer_data:0:review_user"
		rich[role+"_login"] = login
		userData, found, err := getGitHubUser(ctx, login)
		if err != nil {
			lib.Printf("Cannot get %s %s info: %+v while processing %+v\n", login, role, err, prettyPrint(ev))
			return
		}
		if found {
			// name, username, email
			identity := [3]string{"none", login, "none"}
			name := userData["name"]
			email := userData["email"]
			rich[role+"_name"] = name
			if name != nil {
				identity[0] = *name
			}
			if email != nil {
				identity[2] = *email
				ary := strings.Split(*email, "@")
				if len(ary) > 1 {
					rich[role+"_domain"] = ary[1]
				}
			} else {
				rich[role+"_domain"] = nil
			}
			rich[role+"_org"] = userData["company"]
			rich[role+"_location"] = userData["location"]
			rich[role+"_geolocation"] = nil
			addIdentity(ctx, identity)
			identities = append(identities, map[string]interface{}{"name": identity[0], "username": identity[1], "email": identity[2]})
			roles = append(roles, role)
			reviewers = append(reviewers, login)
			// We assume that comment is a review when it has ReviewID or CommitID and its GitHub author is found (found in GitHub, his/her affiliation scan be unknown)
			hasReview = true
		} else if ctx.Debug > 0 {
			lib.Printf("warning: PR %s user %s not found\n", role, login)
			rich[role+"_name"] = nil
			rich[role+"_domain"] = nil
			rich[role+"_org"] = nil
			rich[role+"_location"] = nil
			rich[role+"_geolocation"] = nil
			rich[role+"_login"] = nil
			rich[role+"_data_uuid"] = ""
			rich[role+"_data_user_name"] = ""
			rich[role+"_data_org_name"] = "Unknown"
			rich[role+"_data_name"] = ""
			rich[role+"_data_multi_org_names"] = []string{"Unknown"}
			rich[role+"_data_id"] = ""
			rich[role+"_data_gender_acc"] = nil
			rich[role+"_data_gender"] = ""
			rich[role+"_data_domain"] = ""
			rich[role+"_data_bot"] = false
		}
	}
	rich["reviewers"] = reviewers
	rich["id_in_repo"] = pr.Number
	rich["title"] = pr.Title
	rich["title_analyzed"] = pr.Title
	rich["state"] = pr.State
	rich["pr_state"] = pr.State
	rich["pr_created_at"] = pr.CreatedAt
	rich["created_at"] = pr.CreatedAt
	rich["closed_at"] = pr.ClosedAt
	rich["updated_at"] = pr.UpdatedAt
	rich["pr_closed_at"] = pr.ClosedAt
	rich["pr_updated_at"] = pr.UpdatedAt
	rich["merged_at"] = pr.MergedAt
	rich["merged"] = pr.Merged
	sNumber := strconv.Itoa(pr.Number)
	rich["url"] = repo + "/pull/" + sNumber
	rich["url_id"] = githubRepo + "/pull/" + sNumber
	rich["pr_url"] = rich["url"]
	rich["pr_url_id"] = rich["url_id"]
	rich["all_labels"] = []string{}
	rich["labels"] = []string{}
	rich["comments"] = pr.Comments
	rich["pr_comments"] = pr.Comments
	rich["num_review_comments"] = pr.ReviewComments
	rich["locked"] = pr.Locked
	rich["pr_locked"] = pr.Locked
	if pr.Milestone != nil {
		rich["milestone"] = pr.Milestone.Name
	} else {
		rich["milestone"] = nil
	}
	rich["pr_milestone"] = rich["milestone"]
	rich["commits"] = pr.Commits
	rich["additions"] = pr.Additions
	rich["deletions"] = pr.Deletions
	rich["changed_files"] = pr.ChangedFiles
	if pr.Base.Repo != nil {
		rich["forks"] = pr.Base.Repo.Forks
	} else {
		rich["forks"] = nil
	}
	if pr.MergedAt == nil {
		rich["code_merge_duration"] = nil
	} else {
		rich["code_merge_duration"] = float64(pr.MergedAt.Sub(pr.CreatedAt).Seconds()) / 86400.0
	}
	// FIXME: we don't have this information in GHA
	// rich["time_to_merge_request_response"]
	// Affiliations
	if len(identities) > 0 {
		debugSQL := 0
		if ctx.Debug > 0 {
			debugSQL = 2
		}
		pctx := &dads.Ctx{ProjectSlug: ev.GHAFxSlug, DB: gDadsCtx.DB, AffiliationAPIURL: gDadsCtx.AffiliationAPIURL, Debug: ctx.Debug, DebugSQL: debugSQL}
		dt := ev.CreatedAt
		authorKey := "user_data"
		affsItems := make(map[string]interface{})
		var (
			affsIdentity map[string]interface{}
			empty        bool
			err          error
			t            int
			got          bool
		)
		for i, identity := range identities {
			role := roles[i]
			for {
				affsIdentity, empty, err = dads.IdentityAffsData(pctx, gGitHubDS, identity, nil, dt, role)
				if err != nil {
					if t < 3 {
						t++
						lib.Printf("cannot get affiliations data: %v for %v,%v,%s,%s, retrying after %ds\n", err, identity, dt, role, ev.GHAFxSlug, t)
						time.Sleep(time.Duration(t) * time.Second)
						continue
					}
					lib.Printf("cannot get affiliations data: %v for %v,%v,%s,%s, giving up\n", err, identity, dt, role, ev.GHAFxSlug)
					break
				}
				got = true
				break
			}
			if !got {
				return
			}
			if empty {
				email, _ := identity["email"].(string)
				name, _ := identity["name"].(string)
				username, _ := identity["username"].(string)
				id := dads.UUIDAffs(pctx, "github", email, name, username)
				if id == "" {
					lib.Printf("error: enrichPRData: failed to generate uuid for role %s (%s,github,%s,%s)\n", role, email, name, username)
				}
				if ctx.Debug > 0 {
					lib.Printf("no PR identity affiliation data for %s identity %+v -> pending %s\n", role, identity, id)
				}
				if name == "none" {
					name = ""
				}
				if username == "none" {
					username = ""
				}
				affsIdentity[role+"_id"] = id
				affsIdentity[role+"_uuid"] = id
				affsIdentity[role+"_name"] = name
				affsIdentity[role+"_user_name"] = username
				affsIdentity[role+"_domain"] = dads.IdentityAffsDomain(identity)
				affsIdentity[role+"_gender"] = dads.Unknown
				affsIdentity[role+"_gender_acc"] = nil
				affsIdentity[role+"_org_name"] = dads.Unknown
				affsIdentity[role+"_bot"] = false
				affsIdentity[role+dads.MultiOrgNames] = []interface{}{dads.Unknown}
			}
			for prop, value := range affsIdentity {
				affsItems[prop] = value
			}
			for _, suff := range dads.RequiredAffsFields {
				k := role + suff
				_, ok := affsIdentity[k]
				if !ok {
					affsIdentity[k] = dads.Unknown
				}
			}
		}
		for prop, value := range affsItems {
			setAffsObjectProp(rich, prop, value)
		}
		ks := make(map[string]struct{})
		for k := range rich {
			if strings.HasPrefix(k, "reviewer_data:") {
				ks[k] = struct{}{}
				continue
			}
			for _, pref := range rols {
				if strings.HasPrefix(k, pref[0]+":") {
					ks[k] = struct{}{}
					continue
				}
			}
		}
		for k := range ks {
			delete(rich, k)
		}
		for _, suff := range dads.AffsFields {
			rich["author"+suff] = rich[authorKey+suff]
		}
		orgsKey := authorKey + dads.MultiOrgNames
		_, ok = rich[orgsKey]
		if !ok {
			rich[orgsKey] = []interface{}{}
		}
		rich["author"+dads.MultiOrgNames] = rich[orgsKey]
	}
	// Extra data from comment field
	if hasReview {
		rd, _ := rich["reviewer_data"]
		m, _ := rd.([]interface{})[0].(map[string]interface{})
		// "review_state" : "CHANGES_REQUESTED",
		// "review_html_url" : "https://github.com/code-sleuth/gh-pr-reviewers/pull/1#pullrequestreview-545410679",
		m["review_id"] = comment.ID
		m["review_comment"] = comment.Body
		m["review_commit_id"] = comment.CommitID
		m["review_original_commit_id"] = comment.OriginalCommitID
		m["review_comment"] = comment.Body
		m["review_submitted_at"] = comment.CreatedAt
		m["review_pull_request_review_id"] = comment.PullRequestReviewID
		m["review_line"] = comment.Line
		m["review_path"] = comment.Path
		m["review_position"] = comment.Position
		m["review_original_position"] = comment.OriginalPosition
	}
	objProps := []string{"assignees_data", "requested_reviewers_data", "reviewer_data"}
	for _, objProp := range objProps {
		_, ok := rich[objProp]
		if !ok {
			rich[objProp] = []interface{}{}
		}
		rich["all_"+objProp] = rich[objProp]
	}
	addRichItem(ctx, rich)
	processed = true
	return
}

func setAffsObjectProp(rich map[string]interface{}, prop string, value interface{}) {
	ary := strings.Split(prop, ":")
	if len(ary) != 3 {
		rich[prop] = value
		return
	}
	var (
		iAry []interface{}
		nAry int
	)
	obj := ary[0]
	idx, _ := strconv.Atoi(ary[1])
	prp := ary[2]
	iObj, ok := rich[obj]
	if ok {
		iAry, ok = iObj.([]interface{})
		if !ok {
			lib.Fatalf("property '%s' exists and is not an array while trying to add '%s':%+v to\n%+v\n", obj, prop, value, prettyPrint(rich))
		}
	}
	if !ok {
		rich[obj] = iAry
		nAry = 0
	} else {
		nAry = len(iAry)
	}
	if idx >= nAry {
		miss := (idx - nAry) + 1
		for i := 0; i < miss; i++ {
			iAry = append(iAry, make(map[string]interface{}))
		}
	}
	iAry[idx].(map[string]interface{})[prp] = value
	rich[obj] = iAry
	// fmt.Printf("'%s':'%v' added %v\n", prop, value, rich[obj].([]interface{})[idx].(map[string]interface{})[prp])
}

func enrichRepoData(ctx *lib.Ctx, ev *lib.Event, forkEvent bool, origin string, startDates map[string]map[string]time.Time) (processed bool) {
	fSlug := ev.GHAFxSlug
	suff, ok := ev.GHASuffMap["repository"]
	if !ok {
		if ctx.Debug > 0 {
			lib.Printf("%s: repository not configured\n", origin)
		}
		return
	}
	idx := cPrefix + strings.Replace(fSlug, "/", "-", -1) + "-github-repository" + suff
	var startDate time.Time
	indexStartDates, ok := startDates[idx]
	if ok {
		startDate, ok = indexStartDates[origin]
	}
	hourCreated := lib.HourStart(ev.CreatedAt)
	if ok && startDate.After(hourCreated) {
		if ctx.Debug > 0 {
			lib.Printf("enrichRepoData(%v): %s: %v is after %v, skipping\n", forkEvent, origin, startDate, hourCreated)
		}
		return
	}
	var (
		forksCount   int
		starsCount   int
		size         int
		openIssues   int
		subsCount    int
		fork         bool
		err          error
		artificialID string
	)
	now := time.Now()
	if forkEvent {
		forksCount, starsCount, subsCount, size, openIssues, fork, ok, err = getForksStarsCountAPI(ctx, ev, origin)
		if err != nil {
			lib.Printf("Cannot get %s repo info: %+v while processing %+v\n", origin, err, prettyPrint(ev))
			return
		}
		artificialID = fmt.Sprintf("%s@%d", origin, now.Unix()/60)
	} else {
		forksCount, starsCount, size, openIssues, fork, ok = getForksStarsCount(ctx, ev, origin)
		artificialID = fmt.Sprintf("%s@%d", origin, ev.CreatedAt.UnixNano())
	}
	if !ok {
		if ctx.Debug > 0 {
			lib.Printf("%s: %s: cannot get forks/stargazers info, skipping, forkEvent: %v\n", origin, ev.Type, forkEvent)
		}
		return
	}
	rich := make(map[string]interface{})
	uuid := dads.UUIDNonEmpty(&dads.Ctx{}, origin, artificialID)
	if uuid == "" {
		lib.Printf("error: enrichRepoData: failed to generate uuid for (%s,%s)\n", origin, artificialID)
		return
	}
	repo := "https://github.com/" + origin
	rich["event_type"] = ev.Type
	rich["slug"] = ev.GHAFxSlug
	rich["index"] = idx
	rich["project"] = ev.GHAProj
	rich["project_ts"] = now.Unix()
	rich["gha_hour"] = ev.GHADt
	rich["origin"] = repo
	rich["tag"] = repo
	rich["url"] = repo
	rich["metadata__updated_on"] = ev.CreatedAt
	rich["metadata__timestamp"] = ev.CreatedAt
	rich["grimoire_creation_date"] = ev.CreatedAt
	rich["repo_status_date"] = now
	rich["fetched_on"] = float64(ev.CreatedAt.UnixNano()) / 1.0e9
	rich["uuid"] = uuid
	rich["id"] = artificialID
	rich["is_github_repository"] = 1
	rich["metadata__enriched_on"] = now
	rich["offset"] = nil
	rich["old_fmt"] = false
	rich["forks_count"] = forksCount
	rich["stargazers_count"] = starsCount
	rich["size"] = size
	rich["open_issues"] = openIssues
	rich["fork"] = fork
	githubRepo := origin
	if strings.HasSuffix(githubRepo, ".git") {
		githubRepo = githubRepo[:len(githubRepo)-4]
	}
	rich["github_repo"] = githubRepo
	// FIXME: we only get this data when using GitHub API (ev.Type == "ForkEvent")
	rich["subscribers_count"] = subsCount
	addRichItem(ctx, rich)
	processed = true
	return
}

func enrichRepoDataOld(ctx *lib.Ctx, ev *lib.EventOld, origin string, startDates map[string]map[string]time.Time) (processed bool) {
	fSlug := ev.GHAFxSlug
	suff, ok := ev.GHASuffMap["repository"]
	if !ok {
		if ctx.Debug > 0 {
			lib.Printf("%s: repository not configured\n", origin)
		}
		return
	}
	idx := cPrefix + strings.Replace(fSlug, "/", "-", -1) + "-github-repository" + suff
	var startDate time.Time
	indexStartDates, ok := startDates[idx]
	if ok {
		startDate, ok = indexStartDates[origin]
	}
	hourCreated := lib.HourStart(ev.CreatedAt)
	if ok && startDate.After(hourCreated) {
		if ctx.Debug > 0 {
			lib.Printf("enrichRepoDataOld: %s: %v is after %v, skipping\n", origin, startDate, hourCreated)
		}
		return
	}
	forksCount, starsCount, size, openIssues, fork, ok := getForksStarsCountOld(ctx, ev, origin)
	if !ok {
		if ctx.Debug > 0 {
			lib.Printf("%s: %s: cannot get forks/stargazers info (old format), skipping\n", origin, ev.Type)
		}
		return
	}
	rich := make(map[string]interface{})
	now := time.Now()
	artificialID := fmt.Sprintf("%s@%d", origin, ev.CreatedAt.Unix()/60)
	uuid := dads.UUIDNonEmpty(&dads.Ctx{}, origin, artificialID)
	if uuid == "" {
		lib.Printf("error: enrichRepoDataOld: failed to generate uuid for (%s,%s)\n", origin, artificialID)
		return
	}
	repo := "https://github.com/" + origin
	rich["event_type"] = ev.Type
	rich["slug"] = ev.GHAFxSlug
	rich["index"] = idx
	rich["project"] = ev.GHAProj
	rich["project_ts"] = now.Unix()
	rich["gha_hour"] = ev.GHADt
	rich["origin"] = repo
	rich["tag"] = repo
	rich["url"] = repo
	rich["metadata__updated_on"] = ev.CreatedAt
	rich["metadata__timestamp"] = ev.CreatedAt
	rich["grimoire_creation_date"] = ev.CreatedAt
	rich["repo_status_date"] = ev.CreatedAt
	rich["fetched_on"] = float64(ev.CreatedAt.UnixNano()) / 1.0e9
	rich["uuid"] = uuid
	rich["id"] = artificialID
	rich["is_github_repository"] = 1
	rich["metadata__enriched_on"] = now
	rich["offset"] = nil
	rich["old_fmt"] = true
	rich["forks_count"] = forksCount
	rich["stargazers_count"] = starsCount
	rich["size"] = size
	rich["open_issues"] = openIssues
	rich["fork"] = fork
	githubRepo := origin
	if strings.HasSuffix(githubRepo, ".git") {
		githubRepo = githubRepo[:len(githubRepo)-4]
	}
	rich["github_repo"] = githubRepo
	// FIXME: GHA doesn't have this data
	rich["subscribers_count"] = 0
	addRichItem(ctx, rich)
	//pretty, _ := jsoniter.MarshalIndent(rich, "", "  ")
	//fmt.Printf("\n\n%+v\n\n", string(pretty))
	processed = true
	return
}

func markSyncEvent(ctx *lib.Ctx, origin, fSlug string, ghaDt time.Time, ghaSuffMap map[string]string) {
	repo := origin
	if strings.HasSuffix(repo, ".git") {
		repo = repo[:len(repo)-4]
	}
	// types := []string{"pull_request", "issue", "repository"}
	types := []string{"issue", "repository"}
	indices := []string{}
	for _, typ := range types {
		suff, ok := ghaSuffMap[typ]
		if ok {
			indices = append(indices, cPrefix+strings.Replace(fSlug, "/", "-", -1)+"-github-"+typ+suff)
		}
	}
	if gSyncAllDatesMtx != nil {
		gSyncAllDatesMtx.Lock()
	}
	for _, index := range indices {
		_, ok := gSyncAllDates[index]
		if !ok {
			gSyncAllDates[index] = map[string]time.Time{}
		}
		dt, ok := gSyncAllDates[index][repo]
		if !ok {
			gSyncAllDates[index][repo] = ghaDt
			continue
		}
		if ghaDt.After(dt) {
			gSyncAllDates[index][repo] = ghaDt
		}
	}
	if gSyncAllDatesMtx != nil {
		gSyncAllDatesMtx.Unlock()
	}
}

func enrichData(ctx *lib.Ctx, ev *lib.Event, origin string, startDates map[string]map[string]time.Time) (processed int) {
	//  https://docs.github.com/en/free-pro-team@latest/developers/webhooks-and-events/github-event-types
	//  Possible event types:
	//  CommitCommentEvent
	//  CreateEvent
	//  DeleteEvent
	//  ForkEvent
	//  GollumEvent
	//  IssueCommentEvent -> issue
	//  IssuesEvent -> issue
	//  MemberEvent
	//  PublicEvent
	//  PullRequestEvent -> pr
	//  PullRequestReviewCommentEvent -> pr
	//  PullRequestReviewEvent -> pr
	//  PushEvent
	//  ReleaseEvent
	//  TeamAddEvent
	//  WatchEvent
	switch ev.Type {
	case "IssueCommentEvent", "IssuesEvent":
		if enrichIssueData(ctx, ev, origin, startDates) {
			processed++
		}
	case "PullRequestEvent", "PullRequestReviewEvent", "PullRequestReviewCommentEvent":
		if enrichPRData(ctx, ev, nil, origin, startDates) {
			processed++
		}
		if enrichRepoData(ctx, ev, false, origin, startDates) {
			processed++
		}
	case "ForkEvent":
		if enrichRepoData(ctx, ev, true, origin, startDates) {
			processed++
		}
	}
	markSyncEvent(ctx, origin, ev.GHAFxSlug, ev.GHADt, ev.GHASuffMap)
	return
}

func enrichDataOld(ctx *lib.Ctx, ev *lib.EventOld, origin string, startDates map[string]map[string]time.Time) (processed int) {
	// we don't have enought data to enrich issue type object In pre-2015 event format
	switch ev.Type {
	case "PullRequestEvent", "PullRequestReviewEvent", "PullRequestReviewCommentEvent":
		if enrichPRData(ctx, nil, ev, origin, startDates) {
			processed++
		}
	}
	if enrichRepoDataOld(ctx, ev, origin, startDates) {
		processed++
	}
	markSyncEvent(ctx, origin, ev.GHAFxSlug, ev.GHADt, ev.GHASuffMap)
	return
}

func parseJSON(ctx *lib.Ctx, jsonStr []byte, dt time.Time, config map[[2]string]*regexp.Regexp, dss map[string]map[string]string, startDates map[string]map[string]time.Time, stat map[string]int) (f, r int) {
	var (
		h         lib.Event
		hOld      lib.EventOld
		err       error
		fullName  string
		oldFormat bool
	)
	if dt.Before(gNewFormatStarts) {
		oldFormat = true
		err = jsoniter.Unmarshal(jsonStr, &hOld)
	} else {
		err = jsoniter.Unmarshal(jsonStr, &h)
	}
	// jsonStr = bytes.Replace(jsonStr, []byte("\x00"), []byte(""), -1)
	if err != nil {
		lib.Printf("Error(%v): %v\n", lib.ToGHADate(dt), err)
		lib.Printf("%v: Cannot unmarshal:\n%s\n%v\n", dt, string(jsonStr), err)
		fmt.Fprintf(os.Stderr, "%v: Cannot unmarshal:\n%s\n%v\n", dt, string(jsonStr), err)
		return
	}
	lib.FatalOnError(err)
	if oldFormat {
		fullName = lib.MakeOldRepoName(&hOld.Repository)
	} else {
		fullName = h.Repo.Name
	}
	var hits map[[2]string]struct{}
	if ctx.Debug > 0 {
		hits = make(map[[2]string]struct{})
	}
	for key, re := range config {
		// Do not include all fixtures combined RE
		if key[0] == "" {
			continue
		}
		if !repoHit(fullName, re) {
			continue
		}
		ary := strings.Split(key[0], ":")
		fSlug := ary[0]
		suffMap := dss[fSlug]
		s := fSlug + "/" + key[1]
		n, ok := stat[s]
		if !ok {
			stat[s] = 1
		} else {
			stat[s] = n + 1
		}
		if oldFormat {
			hOld.GHADt = dt
			hOld.GHAFxSlug = fSlug
			hOld.GHASuffMap = suffMap
			hOld.GHAProj = key[1]
			r += enrichDataOld(ctx, &hOld, fullName, startDates)
		} else {
			h.GHADt = dt
			h.GHAFxSlug = fSlug
			h.GHASuffMap = suffMap
			h.GHAProj = key[1]
			r += enrichData(ctx, &h, fullName, startDates)
		}
		f++
		if ctx.Debug > 0 {
			hits[key] = struct{}{}
		}
	}
	if ctx.Debug > 0 && len(hits) > 1 {
		lib.Printf("%s gives multiple projects hits; %+v\n", fullName, hits)
	}
	return
}

func printStatDesc(stat map[string]int) (s string) {
	if len(stat) == 0 {
		return ""
	}
	rev := make(map[int][]string)
	for proj, n := range stat {
		projs, ok := rev[n]
		if !ok {
			rev[n] = []string{proj}
		} else {
			projs = append(projs, proj)
			rev[n] = projs
		}
	}
	ns := []int{}
	for n := range rev {
		ns = append(ns, n)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(ns)))
	for _, n := range ns {
		projs := rev[n]
		sort.Strings(projs)
		s += fmt.Sprintf("%d: %s; ", n, strings.Join(projs, ", "))
	}
	s = s[:len(s)-2]
	return
}

func getGHAJSONs(ch chan *time.Time, ctx *lib.Ctx, dt time.Time, config map[[2]string]*regexp.Regexp, dss map[string]map[string]string, startDates map[string]map[string]time.Time) (pdt *time.Time) {
	defer func() {
		if ch != nil {
			ch <- pdt
		}
	}()
	ky := lib.ToGHADate2(dt)
	var (
		ok    bool
		repos map[string]int
	)
	if gGHAMap != nil {
		repos, ok = gGHAMap[ky]
	}
	if ctx.Debug > 1 {
		lib.Printf("repos@ghahour: %s -> %v(%d)\n", ky, ok, len(repos))
	}
	if ok {
		reAll, _ := config[[2]string{"", ""}]
		hits := false
		for repo := range repos {
			if repoHit(repo, reAll) {
				if ctx.Debug > 1 {
					lib.Printf("%s/%s hits re %v\n", ky, repo, reAll)
				}
				hits = true
				break
			}
		}
		if !hits {
			if ctx.Debug > 0 {
				lib.Printf("no need to process GHA %s: no hits\n", ky)
			}
			return
		}
		needsProcessing := false
		// types := []string{"pull_request", "issue", "repository"}
		types := []string{"issue", "repository"}
		for key, re := range config {
			if needsProcessing {
				break
			}
			// Do not include all fixtures combined RE
			if key[0] == "" {
				continue
			}
			gotIndices := false
			indices := []string{}
			fSlug := ""
			for repo := range repos {
				if needsProcessing {
					break
				}
				if repoHit(repo, re) {
					if !gotIndices {
						ary := strings.Split(key[0], ":")
						fSlug = ary[0]
						suffMap := dss[fSlug]
						for _, typ := range types {
							suff, ok := suffMap[typ]
							if ok {
								idx := cPrefix + strings.Replace(fSlug, "/", "-", -1) + "-github-" + typ + suff
								indices = append(indices, idx)
							}
						}
						gotIndices = true
					}
					if ctx.Debug > 1 {
						lib.Printf("%s/%s check against %v (re: %v)\n", repo, ky, indices, re)
					}
					for _, idx := range indices {
						originStartDates, ok := startDates[idx]
						if !ok {
							if ctx.Debug > 0 {
								lib.Printf("%s repo matches %s/%s, but %s index is missing, GHA %v must be processed\n", repo, fSlug, key[1], idx, ky)
							}
							needsProcessing = true
							break
						}
						startDate, ok := originStartDates[repo]
						if !ok {
							if ctx.Debug > 0 {
								lib.Printf("%s repo matches %s/%s, but %s/%s start date is missing, GHA %v must be processed\n", repo, fSlug, key[1], idx, repo, ky)
							}
							needsProcessing = true
							break
						}
						ghaDate := lib.HourStart(startDate)
						if !dt.Before(ghaDate) {
							if ctx.Debug > 0 {
								lib.Printf("%s repo matches %s/%s in %s, start date is %v, GHA %v must be processed\n", repo, fSlug, key[1], idx, startDate, ky)
							}
							needsProcessing = true
							break
						} else if ctx.Debug > 1 {
							lib.Printf("%s repo matches %s/%s in %s, start date is %v - not before %v, GHA %v can be skipped\n", repo, fSlug, key[1], idx, startDate, dt, ky)
						}
					}
				}
			}
		}
		if !needsProcessing {
			if ctx.Debug > 0 {
				lib.Printf("we don't need to process GHA %s\n", ky)
			}
			return
		}
	}
	fn := fmt.Sprintf("http://data.gharchive.org/%s.json.gz", lib.ToGHADate(dt))
	// Get gzipped JSON array via HTTP
	trials := 0
	httpRetry := 5
	var (
		jsonsBytes  []byte
		nJSONsBytes int64
	)
	handleJSONsBytesLimit(ctx, 0)
	for {
		trials++
		if trials > 1 {
			lib.Printf("Retry(%d) %+v\n", trials, dt)
		}
		httpClient := &http.Client{Timeout: time.Minute * time.Duration(trials*2)}
		response, err := httpClient.Get(fn)
		if err != nil {
			lib.Printf("%v: Error http.Get:\n%v\n", dt, err)
			if trials < httpRetry {
				time.Sleep(time.Duration((1+rand.Intn(20))*trials) * time.Second)
				continue
			}
			fmt.Fprintf(os.Stderr, "%v: Error http.Get:\n%v\n", dt, err)
		}
		lib.FatalOnError(err)
		httpClient = nil

		// Decompress Gzipped response
		reader, err := gzip.NewReader(response.Body)
		// lib.FatalOnError(err)
		if err != nil {
			_ = response.Body.Close()
			lib.Printf("%v: No data yet, gzip reader:\n%v\n", dt, err)
			if trials < httpRetry {
				time.Sleep(time.Duration((1+rand.Intn(3))*trials) * time.Second)
				continue
			}
			fmt.Fprintf(os.Stderr, "%v: No data yet, gzip reader:\n%v\n", dt, err)
			lib.Printf("Gave up on %+v\n", dt)
			return
		}
		lib.Printf("Opened %s\n", fn)

		jsonsBytes, err = ioutil.ReadAll(reader)
		_ = reader.Close()
		_ = response.Body.Close()
		reader = nil
		response = nil
		// lib.FatalOnError(err)
		if err != nil {
			lib.Printf("%v: Error (no data yet, ioutil readall):\n%v\n", dt, err)
			if trials < httpRetry {
				time.Sleep(time.Duration((1+rand.Intn(20))*trials) * time.Second)
				continue
			}
			fmt.Fprintf(os.Stderr, "%v: Error (no data yet, ioutil readall):\n%v\n", dt, err)
			lib.Printf("Gave up on %+v\n", dt)
			return
		}
		nJSONsBytes = int64(len(jsonsBytes))
		handleJSONsBytesLimit(ctx, nJSONsBytes)
		if trials > 1 {
			lib.Printf("Recovered(%d) & decompressed %s (%d bytes)\n", trials, fn, nJSONsBytes)
		} else {
			lib.Printf("Decompressed %s (%d bytes)\n", fn, nJSONsBytes)
		}
		break
	}

	// Split JSON array into separate JSONs
	jsonsArray := bytes.Split(jsonsBytes, []byte("\n"))
	lib.Printf("Split %s, %d JSONs\n", fn, len(jsonsArray))
	jsonsBytes = nil

	// Process JSONs one by one
	n, f, r := 0, 0, 0
	stat := make(map[string]int)
	for _, json := range jsonsArray {
		if len(json) < 1 {
			continue
		}
		fi, ri := parseJSON(ctx, json, dt, config, dss, startDates, stat)
		n++
		f += fi
		r += ri
	}
	handleJSONsBytesLimit(ctx, -nJSONsBytes)
	lib.Printf(
		"Parsed: %s: %d JSONs, found %d matching, enriched %d (%s)\n",
		fn, n, f, r, printStatDesc(stat),
	)
	pdt = &dt
	return
}

func detectMinReposStartDate(ctx *lib.Ctx, config map[[2]string]*regexp.Regexp, dss map[string]map[string]string, startDates map[string]map[string]time.Time) (minFrom time.Time) {
	if ctx.NoGHARepoDates {
		minFrom = gMinGHA
		return
	}
	defer func() { runGC() }()
	minFrom = lib.PrevHourStart(time.Now())
	minRepo := ""
	// types := []string{"pull_request", "issue", "repository"}
	types := []string{"issue", "repository"}
	reAll, _ := config[[2]string{"", ""}]
	rdts := make(map[string]time.Time)
	var rdtsMtx *sync.Mutex
	thrN := gThrN
	if ctx.MaxParallelSHAs > 0 && thrN > ctx.MaxParallelSHAs {
		thrN = ctx.MaxParallelSHAs
	}
	if thrN > 1 {
		rdtsMtx = &sync.Mutex{}
	}
	lib.Printf("generating repo - start date mapping.\n")
	processSHA := func(ch chan struct{}, currSHA string) {
		defer func() {
			if ch != nil {
				ch <- struct{}{}
			}
		}()
		lib.Printf("detectMinReposStartDate: SHA: %s\n", currSHA)
		ghaRepoDates := loadGHARepoDates(ctx, currSHA)
		if ghaRepoDates == nil {
			lib.Printf("No GHA repo dates file for SHA %s, skipping\n", currSHA)
			return
		}
		for org, orgRepos := range ghaRepoDates {
			for r, rdt := range orgRepos {
				var repo string
				if org == "" {
					repo = r
				} else {
					repo = org + "/" + r
				}
				if !repoHit(repo, reAll) {
					continue
				}
				dt := time.Unix(int64(rdt)*int64(3600), 0)
				if rdtsMtx != nil {
					rdtsMtx.Lock()
				}
				rdts[repo] = dt
				if rdtsMtx != nil {
					rdtsMtx.Unlock()
				}
				if ctx.Debug > 0 {
					lib.Printf("%s: added %s with %s start date\n", currSHA, repo, lib.ToGHADate2(dt))
				}
			}
		}
		ghaRepoDates = nil
		runGC()
	}
	if thrN > 1 {
		nThreads := 0
		ch := make(chan struct{})
		for i := 0; i < 0x100; i++ {
			cSHA := fmt.Sprintf("%02x", i)
			go processSHA(ch, cSHA)
			nThreads++
			if nThreads == thrN {
				<-ch
				nThreads--
			}
		}
		for nThreads > 0 {
			<-ch
			nThreads--
		}
	} else {
		for i := 0; i < 0x100; i++ {
			cSHA := fmt.Sprintf("%02x", i)
			processSHA(nil, cSHA)
		}
	}
	lib.Printf("generated repo - start date mapping with %d hits\n", len(rdts))
	runGC()
	for key, re := range config {
		// Do not include all fixtures combined RE
		if key[0] == "" {
			continue
		}
		gotIndices := false
		indices := []string{}
		fSlug := ""
		for repo, dt := range rdts {
			if !repoHit(repo, re) {
				continue
			}
			if !gotIndices {
				ary := strings.Split(key[0], ":")
				fSlug = ary[0]
				suffMap := dss[fSlug]
				for _, typ := range types {
					suff, ok := suffMap[typ]
					if ok {
						idx := cPrefix + strings.Replace(fSlug, "/", "-", -1) + "-github-" + typ + suff
						indices = append(indices, idx)
					}
				}
				gotIndices = true
			}
			if ctx.Debug > 1 {
				lib.Printf("%s check against %v for %v\n", repo, indices, dt)
			}
			for _, idx := range indices {
				originStartDates, ok := startDates[idx]
				if !ok {
					startDates[idx] = make(map[string]time.Time)
					startDates[idx][repo] = dt
					if dt.Before(minFrom) {
						minFrom = dt
						minRepo = idx + "/" + repo
					}
					if ctx.Debug > 0 {
						lib.Printf("%s index was missing, added %s repo with %v start date\n", idx, repo, dt)
					}
					continue
				}
				startDate, ok := originStartDates[repo]
				if !ok {
					startDates[idx][repo] = dt
					if dt.Before(minFrom) {
						minFrom = dt
						minRepo = idx + "/" + repo
					}
					if ctx.Debug > 0 {
						lib.Printf("%s index added %s repo with %v start date\n", idx, repo, dt)
					}
					continue
				}
				if ctx.Debug > 0 {
					lib.Printf("%s index %s repo with %v start date, not updated to %v\n", idx, repo, startDate, dt)
				}
				if startDate.Before(minFrom) {
					minFrom = startDate
					minRepo = idx + "/" + repo
				}
			}
		}
	}
	lib.Printf("detected start date: %v from %s repo\n", minFrom, minRepo)
	return
}

func gha(ctx *lib.Ctx, incremental bool, config map[[2]string]*regexp.Regexp, allRepos []string, startDates map[string]map[string]time.Time) {
	// Environment context parse
	var (
		err      error
		hourFrom int
		hourTo   int
		dFrom    time.Time
		dTo      time.Time
	)
	rand.Seed(time.Now().UnixNano())

	if ctx.Debug > 0 {
		lib.Printf("start dates configuration:\n")
		ks := []string{}
		for k := range startDates {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		for _, k := range ks {
			lib.Printf("%s:\n", k)
			vs := []string{}
			d := startDates[k]
			for v := range d {
				vs = append(vs, v)
			}
			sort.Strings(vs)
			for _, v := range vs {
				lib.Printf("\t%s: %s\n", v, lib.ToYMDHMSDate(d[v]))
			}
		}
	}

	dss := make(map[string]map[string]string)
	for key := range config {
		if key[0] == "" {
			continue
		}
		ary := strings.Split(key[0], ":")
		dss[ary[0]] = getIndexSuffixMap(ary[1])
	}

	if ctx.Debug > 0 {
		lib.Printf("fixtures configuration:\n")
		ks := []string{}
		for k := range config {
			if k[0] == "" {
				continue
			}
			ks = append(ks, k[0]+"###"+k[1])
		}
		sort.Strings(ks)
		for _, s := range ks {
			a := strings.Split(s, "###")
			k := [2]string{a[0], a[1]}
			v := config[k]
			a2 := strings.Split(a[0], ":")
			f := a2[0]
			lib.Printf("%s: %s: %+v, %+v\n", f, a[1], dss[f], v)
		}
	}

	minFrom := time.Now()
	minRepo := ""
	for idx, originStartDates := range startDates {
		for repo, startDate := range originStartDates {
			if startDate.Before(minFrom) {
				minFrom = startDate
				minRepo = idx + "/" + repo
			}
		}
	}
	if minRepo == "" || minFrom.Before(gMinGHA) {
		minFrom = gMinGHA
	}
	if !incremental {
		if minRepo == "" {
			lib.Printf("no start date found across indices\n")
		} else {
			lib.Printf("start date index: %v detected across indices (%s), but it wasn't possible to set autodetected incremental sync mode\n", minRepo, minFrom)
		}
		// minFrom = gMinGHA
		minFrom = detectMinReposStartDate(ctx, config, dss, startDates)
	}

	// Current date
	now := time.Now()
	startD, startH, endD, endH := os.Getenv("GHA_DAY_FROM"), os.Getenv("GHA_HOUR_FROM"), os.Getenv("GHA_DAY_TO"), os.Getenv("GHA_HOUR_TO")
	if startD == "" {
		startD = lib.ToYMDDate(minFrom)
	}
	if endD == "" {
		endD = lib.Today
	}
	if startH == "" {
		startH = strconv.Itoa(minFrom.Hour())
	}
	if endH == "" {
		endH = lib.Now
	}
	lib.Printf("Date range: %s %s - %s %s\n", startD, startH, endD, endH)

	// Parse from day & hour
	if strings.ToLower(startH) == lib.Now {
		hourFrom = now.Hour()
	} else {
		hourFrom, err = strconv.Atoi(startH)
		lib.FatalOnError(err)
	}

	if strings.ToLower(startD) == lib.Today {
		dFrom = lib.DayStart(now).Add(time.Duration(hourFrom) * time.Hour)
	} else {
		dFrom, err = time.Parse(
			time.RFC3339,
			fmt.Sprintf("%sT%02d:00:00+00:00", startD, hourFrom),
		)
		lib.FatalOnError(err)
	}

	// Parse to day & hour
	var (
		currNow       time.Time
		currPeriodEnd time.Time
	)
	dateToFunc := func() {
		currNow = time.Now()
		if strings.ToLower(endH) == lib.Now {
			hourTo = currNow.Hour()
		} else {
			hourTo, err = strconv.Atoi(endH)
			lib.FatalOnError(err)
		}

		if strings.ToLower(endD) == lib.Today {
			dTo = lib.DayStart(currNow).Add(time.Duration(hourTo) * time.Hour)
		} else {
			dTo, err = time.Parse(
				time.RFC3339,
				fmt.Sprintf("%sT%02d:00:00+00:00", endD, hourTo),
			)
			lib.FatalOnError(err)
		}
		if dTo.After(currPeriodEnd) {
			dTo = currPeriodEnd
		}
	}
	currPeriodEnd = lib.PrevHourStart(lib.NextNDaysStart(dFrom, cNDaysGHAPeriod))
	dateToFunc()
	lib.Printf("Date range: %s - %s\n", lib.ToGHADate2(dFrom), lib.ToGHADate2(dTo))

	defer func() {
		uploadIdentities(ctx, &gDadsCtx, false)
		uploadRichItems(ctx, false)
		enchanceStartDates(ctx, startDates, false)
	}()

	ic := 0
	maybeGCAndEnchance := func() {
		ic++
		if ic%6 == 0 {
			runGC()
		}
		// FIXME: this updates start dates to the future, so breaks enrichment - need to fix this somehow
		//if ic%360 == 0 {
		//	enchanceStartDates(ctx, startDates, true)
		//}
	}

	maxProcessed := gMinGHA
	dt := dFrom
	for {
		currPeriodEnd = lib.PrevHourStart(lib.NextNDaysStart(dt, cNDaysGHAPeriod))
		dateToFunc()
		lib.Printf("Processing period (%d days) %s - %s\n", cNDaysGHAPeriod, lib.ToGHADate(dt), lib.ToGHADate(currPeriodEnd))
		if !ctx.NoGHAMap {
			loadGHAMap(ctx, dt)
			if gGHAMap == nil {
				lib.Printf("warning, no GHA map file for %s, doing a full scan\n", lib.ToGHADate(dt))
			}
		}
		if gThrN > 1 {
			ch := make(chan *time.Time)
			nThreads := 0
			for dt.Before(dTo) || dt.Equal(dTo) {
				dateToFunc()
				go getGHAJSONs(ch, ctx, dt, config, dss, startDates)
				dt = dt.Add(time.Hour)
				nThreads++
				if nThreads == gThrN {
					pdt := <-ch
					nThreads--
					dateToFunc()
					if pdt != nil && pdt.After(maxProcessed) {
						maxProcessed = *pdt
						maybeGCAndEnchance()
					}
				}
			}
			for nThreads > 0 {
				pdt := <-ch
				nThreads--
				dateToFunc()
				if pdt != nil && pdt.After(maxProcessed) {
					maxProcessed = *pdt
					maybeGCAndEnchance()
				}
			}
		} else {
			for dt.Before(dTo) || dt.Equal(dTo) {
				dateToFunc()
				pdt := getGHAJSONs(nil, ctx, dt, config, dss, startDates)
				dt = dt.Add(time.Hour)
				if pdt != nil && pdt.After(maxProcessed) {
					maxProcessed = *pdt
					maybeGCAndEnchance()
				}
			}
		}
		// Uncomment to update repo start dates when processing actual GHA data
		// updateGHARepoDates(ctx)
		dt = lib.NextHourStart(currPeriodEnd)
		if !dt.Before(time.Now()) {
			break
		}
	}
	if maxProcessed.After(gMinGHA) {
		currentConfig := serializeConfig(config)
		err = saveFixturesState(ctx, currentConfig, allRepos, maxProcessed)
		if err != nil {
			lib.Printf("cannot save sync info: %+v\n", err)
			return
		}
	} else {
		lib.Printf("no new data was processed, not saving fixtures state\n")
	}
}

func enchanceStartDates(ctx *lib.Ctx, startDates map[string]map[string]time.Time, withMtx bool) {
	updateStartDates(startDates, gSyncDates)
	if withMtx && gSyncAllDatesMtx != nil {
		gSyncAllDatesMtx.Lock()
	}
	addStartDates(startDates, gSyncAllDates)
	if withMtx && gSyncAllDatesMtx != nil {
		gSyncAllDatesMtx.Unlock()
	}
	lib.FatalOnError(saveConfigStartDates(ctx, startDates))
}

func getOriginStartDates(ctx *lib.Ctx, idx string) (startDates map[string]time.Time) {
	// curl -XPOST -H 'Content-type: application/json' URL/_sql?format=csv -d"{\"query\":\"select origin, max(metadata__updated_on) from \\\"idx\\\" group by origin\"}"
	data := fmt.Sprintf(
		//`{"query":"select origin, max(metadata__updated_on) as date from \"%s\" group by origin","fetch_size":%d}`,
		//`{"query":"select origin, max(metadata__enriched_on) as date from \"%s\" group by origin","fetch_size":%d}`,
		`{"query":"select origin, max(gha_hour) as date from \"%s\" group by origin","fetch_size":%d}`,
		idx,
		10000,
	)
	payloadBytes := []byte(data)
	payloadBody := bytes.NewReader(payloadBytes)
	method := "POST"
	url := fmt.Sprintf("%s/_sql?format=json", ctx.ESURL)
	req, err := http.NewRequest(method, url, payloadBody)
	if err != nil {
		if ctx.Debug > 0 {
			lib.Printf("new request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
		}
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		if ctx.Debug > 0 {
			lib.Printf("do request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
		}
		return
	}
	var body []byte
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		if ctx.Debug > 0 {
			lib.Printf("ReadAll non-ok request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
		}
		return
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		if ctx.Debug > 0 {
			lib.Printf("Method:%s url:%s data: %s status:%d\n%s\n", method, url, data, resp.StatusCode, body)
		}
		return
	}
	type resultType struct {
		Cursor string     `json:"cursor"`
		Rows   [][]string `json:"rows"`
	}
	var result resultType
	err = jsoniter.Unmarshal(body, &result)
	if err != nil {
		if ctx.Debug > 0 {
			lib.Printf("Unmarshal error: %+v", err)
		}
		return
	}
	if len(result.Rows) == 0 {
		return
	}
	processResults := func() {
		for _, row := range result.Rows {
			ary := strings.Split(row[0], "/")
			lAry := len(ary)
			if lAry < 2 {
				continue
			}
			origin := ary[lAry-2] + "/" + ary[lAry-1]
			date, err := lib.TimeParseES(row[1])
			if err == nil {
				if startDates == nil {
					startDates = make(map[string]time.Time)
				}
				startDates[origin] = date
			}
		}
	}
	processResults()
	for {
		data = `{"cursor":"` + result.Cursor + `"}`
		payloadBytes = []byte(data)
		payloadBody = bytes.NewReader(payloadBytes)
		req, err = http.NewRequest(method, url, payloadBody)
		if err != nil {
			if ctx.Debug > 0 {
				lib.Printf("new request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
			}
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			if ctx.Debug > 0 {
				lib.Printf("do request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
			}
			return
		}
		body, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			if ctx.Debug > 0 {
				lib.Printf("ReadAll non-ok request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
			}
			return
		}
		_ = resp.Body.Close()
		if resp.StatusCode != 200 {
			if ctx.Debug > 0 {
				lib.Printf("Method:%s url:%s data: %s status:%d\n%s\n", method, url, data, resp.StatusCode, body)
			}
			return
		}
		err = jsoniter.Unmarshal(body, &result)
		if err != nil {
			if ctx.Debug > 0 {
				lib.Printf("Unmarshal error: %+v", err)
			}
			return
		}
		if len(result.Rows) == 0 {
			break
		}
		processResults()
	}
	url = fmt.Sprintf("%s/_sql/close", ctx.ESURL)
	data = `{"cursor":"` + result.Cursor + `"}`
	payloadBytes = []byte(data)
	payloadBody = bytes.NewReader(payloadBytes)
	req, err = http.NewRequest(method, url, payloadBody)
	if err != nil {
		if ctx.Debug > 0 {
			lib.Printf("new request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
		}
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		if ctx.Debug > 0 {
			lib.Printf("do request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
		}
		return
	}
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		if ctx.Debug > 0 {
			lib.Printf("ReadAll non-ok request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
		}
		return
	}
	_ = resp.Body.Close()
	if resp.StatusCode != 200 {
		if ctx.Debug > 0 {
			lib.Printf("Method:%s url:%s data: %s status:%d\n%s\n", method, url, data, resp.StatusCode, body)
		}
		return
	}
	return
}

func getIndexSuffixMap(data string) (suffMap map[string]string) {
	suffMap = make(map[string]string)
	ary := strings.Split(data, ",")
	for _, item := range ary {
		a := strings.Split(item, "=")
		suffMap[a[0]] = a[1]
	}
	return
}

func updateStartDates(startDates, update map[string]map[string]time.Time) {
	n, u := 0, 0
	for idx, data := range update {
		_, ok := startDates[idx]
		if !ok {
			startDates[idx] = map[string]time.Time{}
		}
		for origin, newDt := range data {
			dt, ok := startDates[idx][origin]
			if !ok {
				startDates[idx][origin] = newDt
				n++
				continue
			}
			if newDt.After(dt) {
				startDates[idx][origin] = newDt
				u++
			}
		}
	}
	lib.Printf("updated %d sync dates, added %d\n", u, n)
}

func addStartDates(startDates, add map[string]map[string]time.Time) {
	a := 0
	for idx, data := range add {
		_, ok := startDates[idx]
		if !ok {
			startDates[idx] = map[string]time.Time{}
			for origin, dt := range data {
				startDates[idx][origin] = dt
				a++
			}
		} else {
			for origin, dt := range data {
				_, ok := startDates[idx][origin]
				if !ok {
					startDates[idx][origin] = dt
					a++
				}
			}
		}
	}
	lib.Printf("added %d sync dates\n", a)
}

func getStartDates(ctx *lib.Ctx, config map[[2]string]*regexp.Regexp) (startDates map[string]map[string]time.Time) {
	indices := make(map[string]struct{})
	for k := range config {
		fSlug := k[0]
		if fSlug == "" {
			continue
		}
		ary := strings.Split(fSlug, ":")
		fSlug = ary[0]
		suffMap := getIndexSuffixMap(ary[1])
		// for _, typ := range []string{"pull_request", "issue", "repository"} {
		for _, typ := range []string{"issue", "repository"} {
			suff, ok := suffMap[typ]
			if ok {
				indices[cPrefix+strings.Replace(fSlug, "/", "-", -1)+"-github-"+typ+suff] = struct{}{}
			}
		}
	}
	type startDateType struct {
		index       string
		originDates map[string]time.Time
	}
	getStartDate := func(ch chan startDateType, ctx *lib.Ctx, idx string) (startDate startDateType) {
		defer func() {
			if ch != nil {
				ch <- startDate
			}
		}()
		startDate.index = idx
		startDate.originDates = getOriginStartDates(ctx, idx)
		return
	}
	startDates = make(map[string]map[string]time.Time)
	if gThrN > 1 {
		nThreads := 0
		ch := make(chan startDateType)
		for idx := range indices {
			go getStartDate(ch, ctx, idx)
			nThreads++
			if nThreads == gThrN {
				startDate := <-ch
				if startDate.originDates != nil {
					startDates[startDate.index] = startDate.originDates
				}
				nThreads--
			}
		}
		for nThreads > 0 {
			startDate := <-ch
			if startDate.originDates != nil {
				startDates[startDate.index] = startDate.originDates
			}
			nThreads--
		}
	} else {
		for idx := range indices {
			startDate := getStartDate(nil, ctx, idx)
			if startDate.originDates != nil {
				startDates[startDate.index] = startDate.originDates
			}
		}
	}
	savedStartDates, err := loadConfigStartDates(ctx)
	if err == nil {
		updateStartDates(startDates, savedStartDates)
	}
	// Eventually save config start dates
	if ctx.SaveConfig {
		lib.FatalOnError(saveConfigStartDates(ctx, startDates))
	}
	return
}

func ensureSyncInfoIndex(ctx *lib.Ctx) (err error) {
	idx := cPrefix + "da-ds-gha-sync-info"
	method := "HEAD"
	url := ctx.ESURL + "/" + idx
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		if ctx.Debug > 0 {
			lib.Printf("new request error: %+v for %s url: %s\n", err, method, url)
		}
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		if ctx.Debug > 0 {
			lib.Printf("do request error: %+v for %s url: %s\n", err, method, url)
		}
		return
	}
	if resp.StatusCode != 200 {
		if ctx.Debug > 0 {
			lib.Printf("check SHA sync info exists %s --> %d\n", idx, resp.StatusCode)
		}
		method = "PUT"
		var req *http.Request
		req, err = http.NewRequest(method, url, nil)
		if err != nil {
			if ctx.Debug > 0 {
				lib.Printf("new request error: %+v for %s url: %s\n", err, method, url)
			}
			return
		}
		var resp *http.Response
		resp, err = http.DefaultClient.Do(req)
		if err != nil {
			if ctx.Debug > 0 {
				lib.Printf("do request error: %+v for %s url: %s\n", err, method, url)
			}
			return
		}
		if ctx.Debug > 0 {
			lib.Printf("create %s --> %d\n", idx, resp.StatusCode)
		}
	}
	return
}

func saveFixturesState(ctx *lib.Ctx, serializedConfig map[string]string, allRepos []string, tm time.Time) (err error) {
	idx := cPrefix + "da-ds-gha-sync-info"
	method := "PUT"
	item := map[string]interface{}{}
	id := tm.UnixNano()
	sid := fmt.Sprintf("%d", id)
	item["id"] = id
	item["dt"] = tm
	item["config"] = serializedConfig
	item["repos"] = allRepos
	doc, err := jsoniter.Marshal(item)
	if err != nil {
		lib.Printf("marshal config error: %+v\n", err)
		return
	}
	url := ctx.ESURL + "/" + idx + "/_doc/" + sid
	payloadBody := bytes.NewReader(doc)
	var req *http.Request
	req, err = http.NewRequest(method, url, payloadBody)
	if err != nil {
		lib.Printf("new request error: %+v for %s url: %s, doc: %s\n", err, method, url, prettyPrint(item))
		return
	}
	req.Header.Set("Content-Type", "application/json")
	var resp *http.Response
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		lib.Printf("do request error: %+v for %s url: %s, doc: %s\n", err, method, url, prettyPrint(item))
		return
	}
	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		var body []byte
		body, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			lib.Printf("read all response error: %+v for %s url: %s, doc: %s\n", err, method, url, prettyPrint(item))
			return
		}
		_ = resp.Body.Close()
		var result map[string]interface{}
		err = jsoniter.Unmarshal(body, &result)
		if err != nil {
			lib.Printf("unmarshal response error: %+v for %s url: %s, doc: %s, body: %s\n", err, method, url, prettyPrint(item), string(body))
			return
		}
		lib.Printf("save state status %d for %s url: %s, doc: %s, result: %s\n", resp.StatusCode, method, url, prettyPrint(item), prettyPrint(result))
		return
	}
	lib.Printf("saved fixtures state to ES\n")
	return
}

func loadFixturesState(ctx *lib.Ctx) (config map[string]string, allRepos []string, when time.Time, loaded bool, err error) {
	idx := cPrefix + "da-ds-gha-sync-info"
	method := "GET"
	payloadBytes := []byte(`{"query":{"match_all":{}},"sort":{"id":{"order":"desc"}},"size":1}`)
	payloadBody := bytes.NewReader(payloadBytes)
	url := ctx.ESURL + "/" + idx + "/_search"
	var req *http.Request
	req, err = http.NewRequest(method, url, payloadBody)
	if err != nil {
		data := string(payloadBytes)
		lib.Printf("new request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	var resp *http.Response
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		data := string(payloadBytes)
		lib.Printf("do request error: %+v for %s url: %s, data: %s\n", err, method, url, data)
		return
	}
	var body []byte
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		data := string(payloadBytes)
		lib.Printf("read all response error: %+v for %s url: %s, data: %s\n", err, method, url, data)
		return
	}
	_ = resp.Body.Close()
	var result map[string]interface{}
	err = jsoniter.Unmarshal(body, &result)
	if err != nil {
		data := string(payloadBytes)
		lib.Printf("unmarshal response error: %+v for %s url: %s, data: %s, body: %s\n", err, method, url, data, string(body))
		return
	}
	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		data := string(payloadBytes)
		lib.Printf("load state status %d for %s url: %s, data: %s, result: %s\n", resp.StatusCode, method, url, data, prettyPrint(result))
		return
	}
	hits, ok := result["hits"].(map[string]interface{})
	if !ok {
		return
	}
	hits2, ok := hits["hits"].([]interface{})
	if !ok {
		return
	}
	if len(hits2) < 1 {
		return
	}
	doc, ok := hits2[0].(map[string]interface{})
	if !ok {
		return
	}
	item, ok := doc["_source"].(map[string]interface{})
	if !ok {
		return
	}
	sWhen, ok := item["dt"].(string)
	if !ok {
		return
	}
	when, err = lib.TimeParseES(sWhen)
	if err != nil {
		return
	}
	iConfig, ok := item["config"].(map[string]interface{})
	if !ok {
		return
	}
	config = make(map[string]string)
	for k, v := range iConfig {
		config[k], ok = v.(string)
		if !ok {
			return
		}
	}
	iRepos, ok := item["repos"].([]interface{})
	if !ok {
		return
	}
	for _, ir := range iRepos {
		repo, ok := ir.(string)
		if !ok {
			return
		}
		allRepos = append(allRepos, repo)
	}
	// lib.Printf("loaded repos %+v (%d)\n", allRepos, len(allRepos))
	loaded = true
	return
}

// return true if we can do incremental sync
func handleIncremental(ctx *lib.Ctx, config map[[2]string]*regexp.Regexp, allRepos []string, startDates map[string]map[string]time.Time) bool {
	// No incremental flag - skips detecting what needs to be synced
	// LoadConfig mode loads configuration from JSON (this is for the development)
	// This configuration can have nothing to do with the real world, so do not attempt to
	// detect incremental sync state using this
	// TODO: if ctx.NoIncremental || ctx.LoadConfig {
	if ctx.NoIncremental {
		lib.Printf("skipping incremental sync mode\n")
		return false
	}
	err := ensureSyncInfoIndex(ctx)
	if err != nil {
		lib.Printf("cannot ensure sync info index, will do a full sync: %+v\n", err)
		return false
	}
	currentConfig := serializeConfig(config)
	var (
		savedConfig map[string]string
		savedRepos  []string
		ok          bool
		whenSaved   time.Time
	)
	savedConfig, savedRepos, whenSaved, ok, err = loadFixturesState(ctx)
	if err != nil {
		lib.Printf("cannot load sync info, will do a full sync: %+v\n", err)
		return false
	}
	if !ok {
		lib.Printf("no sync info saved yet, doing a full sync\n")
		return false
	}
	savedAllRE, ok := savedConfig[": "]
	if !ok {
		lib.Printf("incorrect saved state, skipping: %s\n", prettyPrint(savedConfig))
		return false
	}
	currentAllRE, ok := currentConfig[": "]
	if !ok {
		lib.Printf("incorrect current state, skipping: %s\n", prettyPrint(currentConfig))
		return false
	}
	// syncFrom := lib.PrevHourStart(whenSaved)
	syncFrom := lib.HourStart(whenSaved)
	// If All RE is the same, then the configuration didn't changed since last run
	if savedAllRE == currentAllRE {
		//savedReposStr := strings.Join(savedRepos, ",")
		//allReposStr := strings.Join(allRepos, ",")
		//reposEqual = savedReposStr == allReposStr
		reposEqual := true
		previousStateMap := map[string]struct{}{}
		for _, repo := range savedRepos {
			previousStateMap[repo] = struct{}{}
		}
		// If any of current repos is not present in previous repos
		// That means we have a new repo, so we need to detect start date (fixtures state changed)
		// If any of previous repos is not present in current repos - this is fine
		// It means we now track less repos, so we can assume no fixtures state was changed
		for _, repo := range allRepos {
			_, ok := previousStateMap[repo]
			if !ok {
				reposEqual = false
				break
			}
		}
		if reposEqual {
			lib.Printf("no fixtures state was changed since %+v\n", whenSaved)
			for idx, originStartDates := range startDates {
				for origin, startDate := range originStartDates {
					if startDate.Before(syncFrom) {
						if ctx.Debug > 0 {
							lib.Printf("updating %s/%s start date %v -> %v\n", idx, origin, startDate, syncFrom)
						}
						startDates[idx][origin] = syncFrom
					}
				}
			}
			return true
		}
	}
	lib.Printf("fixtures state changed\n")
	lib.Printf("saved: %s\n", savedAllRE)
	lib.Printf("saved repos: %v\n", savedRepos)
	lib.Printf("current: %s\n", currentAllRE)
	lib.Printf("current repos: %v\n", allRepos)
	lib.Printf("skipping incremental mode, switching to detecting start date\n")
	return false
}

func previewJSON(ctx *lib.Ctx, jsonStr []byte, dt time.Time, repos map[string]int) {
	var (
		h         lib.Event
		hOld      lib.EventOld
		err       error
		fullName  string
		oldFormat bool
	)
	if dt.Before(gNewFormatStarts) {
		oldFormat = true
		err = jsoniter.Unmarshal(jsonStr, &hOld)
	} else {
		err = jsoniter.Unmarshal(jsonStr, &h)
	}
	if err != nil {
		lib.Printf("Error(%v): %v\n", lib.ToGHADate(dt), err)
		lib.Printf("%v: Cannot unmarshal:\n%s\n%v\n", dt, string(jsonStr), err)
		fmt.Fprintf(os.Stderr, "%v: Cannot unmarshal:\n%s\n%v\n", dt, string(jsonStr), err)
		return
	}
	lib.FatalOnError(err)
	if oldFormat {
		fullName = lib.MakeOldRepoName(&hOld.Repository)
	} else {
		fullName = h.Repo.Name
	}
	repo, _ := repos[fullName]
	repos[fullName] = repo + 1
	return
}

func previewGHAJSONs(ch chan ghaMapItem, ctx *lib.Ctx, dt time.Time) (item ghaMapItem) {
	key := lib.ToGHADate2(dt)
	repos := make(map[string]int)
	defer func() {
		item.dt = dt
		item.key = key
		item.repos = repos
		item.ok = len(repos) > 0
		if ch != nil {
			ch <- item
		}
	}()
	fn := fmt.Sprintf("http://data.gharchive.org/%s.json.gz", lib.ToGHADate(dt))
	// lib.Printf("previewGHAJSONs: %v\n", dt)

	// Get gzipped JSON array via HTTP
	trials := 0
	httpRetry := 5
	var (
		jsonsBytes  []byte
		nJSONsBytes int64
	)
	handleJSONsBytesLimit(ctx, 0)
	for {
		trials++
		if trials > 1 {
			lib.Printf("Retry(%d) %+v\n", trials, dt)
		}
		httpClient := &http.Client{Timeout: time.Minute * time.Duration(trials*2)}
		response, err := httpClient.Get(fn)
		if err != nil {
			lib.Printf("%v: Error http.Get:\n%v\n", dt, err)
			if trials < httpRetry {
				time.Sleep(time.Duration((1+rand.Intn(20))*trials) * time.Second)
				continue
			}
			fmt.Fprintf(os.Stderr, "%v: Error http.Get:\n%v\n", dt, err)
		}
		lib.FatalOnError(err)
		httpClient = nil

		// Decompress Gzipped response
		reader, err := gzip.NewReader(response.Body)
		// lib.FatalOnError(err)
		if err != nil {
			_ = response.Body.Close()
			lib.Printf("%v: No data yet, gzip reader:\n%v\n", dt, err)
			if trials < httpRetry {
				time.Sleep(time.Duration((1+rand.Intn(3))*trials) * time.Second)
				continue
			}
			fmt.Fprintf(os.Stderr, "%v: No data yet, gzip reader:\n%v\n", dt, err)
			lib.Printf("Gave up on %+v\n", dt)
			return
		}
		lib.Printf("Opened %s\n", fn)

		jsonsBytes, err = ioutil.ReadAll(reader)
		_ = reader.Close()
		_ = response.Body.Close()
		reader = nil
		response = nil
		// lib.FatalOnError(err)
		if err != nil {
			lib.Printf("%v: Error (no data yet, ioutil readall):\n%v\n", dt, err)
			if trials < httpRetry {
				time.Sleep(time.Duration((1+rand.Intn(20))*trials) * time.Second)
				continue
			}
			fmt.Fprintf(os.Stderr, "%v: Error (no data yet, ioutil readall):\n%v\n", dt, err)
			lib.Printf("Gave up on %+v\n", dt)
			return
		}
		nJSONsBytes = int64(len(jsonsBytes))
		handleJSONsBytesLimit(ctx, nJSONsBytes)
		if trials > 1 {
			lib.Printf("Recovered(%d) & decompressed %s (%d bytes)\n", trials, fn, nJSONsBytes)
		} else {
			lib.Printf("Decompressed %s (%d bytes)\n", fn, nJSONsBytes)
		}
		break
	}

	// Split JSON array into separate JSONs
	jsonsArray := bytes.Split(jsonsBytes, []byte("\n"))
	lib.Printf("Split %s, %d JSONs\n", fn, len(jsonsArray))
	jsonsBytes = nil

	// Process JSONs one by one
	n := 0
	for _, json := range jsonsArray {
		if len(json) < 1 {
			continue
		}
		previewJSON(ctx, json, dt, repos)
		n++
	}
	handleJSONsBytesLimit(ctx, -nJSONsBytes)
	lib.Printf("Previewed: %s: %d JSONs, %d repos\n", fn, n, len(repos))
	return
}

func handleJSONsBytesLimit(ctx *lib.Ctx, diff int64) {
	if diff == 0 {
		// Check status mode
		waits := 0
		for {
			if gJSONsL2Mtx != nil {
				gJSONsL2Mtx.Lock()
			}
			locked := gJSONsLocked
			if gJSONsL2Mtx != nil {
				gJSONsL2Mtx.Unlock()
			}
			if !locked {
				break
			}
			waits++
			if gJSONsBytesMtx != nil {
				gJSONsBytesMtx.Lock()
			}
			lib.Printf("JSONs uncompressing locked, current size %dM exceed %dM (biggest one seen was/is %dM), sleeping...\n", gAllJSONsBytes>>20, ctx.MaxJSONsBytes>>20, gMaxJSONsBytes>>20)
			if gJSONsBytesMtx != nil {
				gJSONsBytesMtx.Unlock()
			}
			time.Sleep(time.Duration(2) * time.Second)
		}
		if waits > 0 {
			lib.Printf("JSONs unlocked, current size %dM below %dM, waited %d times\n", gAllJSONsBytes>>20, ctx.MaxJSONsBytes>>20, waits)
		}
		return
	}
	s := ""
	lock := false
	unlock := false
	mdiff := -diff
	if gJSONsBytesMtx != nil {
		gJSONsBytesMtx.Lock()
	}
	if diff > 0 {
		if diff > gMaxJSONsBytes {
			gMaxJSONsBytes = diff
			s += fmt.Sprintf("new biggest JSONs size: %d\n", diff)
		}
		if ctx.MaxJSONsBytes > 0 {
			if gAllJSONsBytes > ctx.MaxJSONsBytes {
				s += fmt.Sprintf("processing JSONs %dM even without a new one %dM exceed limit %dM, blocking\n", gAllJSONsBytes>>20, diff>>20, ctx.MaxJSONsBytes>>20)
				lock = true
			} else if diff+gAllJSONsBytes > ctx.MaxJSONsBytes {
				s += fmt.Sprintf("currently processing JSONs %dM and a new one %dM exceed limit %dM, blocking\n", gAllJSONsBytes>>20, diff>>20, ctx.MaxJSONsBytes>>20)
				lock = true
			}
			gAllJSONsBytes += diff
		}
	} else {
		if ctx.MaxJSONsBytes > 0 {
			if gAllJSONsBytes+diff <= ctx.MaxJSONsBytes {
				if gAllJSONsBytes > ctx.MaxJSONsBytes {
					s += fmt.Sprintf("processed JSON %dM frees enough memory from %dM to unblock limit %dM\n", mdiff>>20, gAllJSONsBytes>>20, ctx.MaxJSONsBytes>>20)
				}
				unlock = true
			}
			gAllJSONsBytes += diff
		}
	}
	if gJSONsBytesMtx != nil {
		gJSONsBytesMtx.Unlock()
	}
	if gJSONsLockMtx != nil {
		if unlock {
			if gJSONsL2Mtx != nil {
				gJSONsL2Mtx.Lock()
			}
			if gJSONsLocked {
				gJSONsLockMtx.Unlock()
				gJSONsLocked = false
				s += "JSONs unlocked\n"
			}
			if gJSONsL2Mtx != nil {
				gJSONsL2Mtx.Unlock()
			}
		}
		if lock {
			if gJSONsL2Mtx != nil {
				gJSONsL2Mtx.Lock()
			}
			if !gJSONsLocked {
				gJSONsLockMtx.Lock()
				gJSONsLocked = true
				s += "JSONs locked\n"
			}
			if gJSONsL2Mtx != nil {
				gJSONsL2Mtx.Unlock()
			}
		}
	}
	if s != "" {
		s = strings.Join(strings.Split(s, "\n"), ", ")
		if strings.HasSuffix(s, ", ") {
			s = s[:len(s)-2]
		}
		lib.Printf("%s\n", s)
	}
}

func generateGHAMap(ctx *lib.Ctx, from *time.Time, save, detect, untilNow bool) (changed bool) {
	defer func() { runGC() }()
	if gGHAMap == nil {
		gGHAMap = make(map[string]map[string]int)
		changed = true
	}
	var (
		dDtFrom time.Time
		dDtTo   time.Time
		had     int
		have    int
	)
	if detect {
		had = len(gGHAMap)
	}
	if from == nil {
		dDtFrom = gMinGHA
		// dFrom = time.Date(2020, 12, 10, 0, 0, 0, 0, time.UTC)
	} else {
		dDtFrom = lib.NextHourStart(*from)
	}
	if untilNow {
		dDtTo = lib.PrevHourStart(time.Now())
	} else {
		dDtTo = lib.PrevHourStart(lib.NextNDaysStart(dDtFrom, cNDaysGHAPeriod))
		lastGHAHour := lib.PrevHourStart(time.Now())
		if dDtTo.After(lastGHAHour) {
			dDtTo = lastGHAHour
		}
	}
	igc := 0
	maybeGC := func() {
		igc++
		if igc%24 == 0 {
			runGC()
		}
	}
	dFrom := dDtFrom
	oks := 0
	for {
		dTo := lib.PrevHourStart(lib.NextNDaysStart(dFrom, cNDaysGHAPeriod))
		if dTo.After(dDtTo) {
			dTo = dDtTo
		}
		lib.Printf("generating GHA map (%d days) %s - %s\n", cNDaysGHAPeriod, lib.ToGHADate(dFrom), lib.ToGHADate(dTo))
		if !detect {
			gGHAMap = nil
			gGHAMap = make(map[string]map[string]int)
		}
		dt := dFrom
		if gThrN > 1 {
			ch := make(chan ghaMapItem)
			nThreads := 0
			for dt.Before(dTo) || dt.Equal(dTo) {
				go previewGHAJSONs(ch, ctx, dt)
				dt = dt.Add(time.Hour)
				nThreads++
				if nThreads == gThrN {
					data := <-ch
					nThreads--
					if data.ok {
						gGHAMap[data.key] = data.repos
						maybeGC()
						oks++
					}
				}
			}
			for nThreads > 0 {
				data := <-ch
				nThreads--
				if data.ok {
					gGHAMap[data.key] = data.repos
					maybeGC()
					oks++
				}
			}
		} else {
			for dt.Before(dTo) || dt.Equal(dTo) {
				data := previewGHAJSONs(nil, ctx, dt)
				if data.ok {
					gGHAMap[data.key] = data.repos
					maybeGC()
					oks++
				}
				dt = dt.Add(time.Hour)
			}
		}
		if detect {
			have = len(gGHAMap)
		}
		if save {
			saveGHAMap(ctx, dFrom)
		}
		if oks > 0 {
			updateGHARepoDates(ctx)
		}
		dFrom = lib.NextNDaysStart(dFrom, cNDaysGHAPeriod)
		if !dFrom.Before(dDtTo) {
			break
		}
	}
	if detect {
		changed = have != had
		lib.Printf("generated GHA map %d -> %d items, changed %v\n", had, have, changed)
	}
	return
}

func loadGHAMap(ctx *lib.Ctx, dt time.Time) {
	if ctx.NoGHAMap {
		return
	}
	gGHAMap = nil
	fdt := lib.NDaysStart(dt, cNDaysGHAPeriod)
	sdt := lib.ToPeriodDate(fdt, cNDaysGHAPeriod)
	path := "gha_map/" + sdt + ".json"
	lib.Printf("loading GHA map %s\n", path)
	bts, err := ioutil.ReadFile(path)
	if err != nil {
		lib.Printf("cannot read GHA map file %s\n", path)
		return
	}
	defer func() { runGC() }()
	gGHAMap = make(map[string]map[string]int)
	err = jsoniter.Unmarshal(bts, &gGHAMap)
	if err != nil {
		lib.Printf("cannot unmarshal from GHA map file %s, %d bytes\n", path, len(bts))
		return
	}
	lib.Printf("loaded GHA map %s %d items\n", path, len(gGHAMap))
	return
}

func saveGHAMap(ctx *lib.Ctx, dt time.Time) {
	if gGHAMap == nil {
		return
	}
	if len(gGHAMap) < 1 {
		return
	}
	defer func() { runGC() }()
	fdt := lib.NDaysStart(dt, cNDaysGHAPeriod)
	sdt := lib.ToPeriodDate(fdt, cNDaysGHAPeriod)
	path := "gha_map/" + sdt + ".json"
	runGC()
	bts, err := jsoniter.Marshal(gGHAMap)
	if err != nil {
		lib.Printf("cannot marshal GHA map with %d items to file %s\n", len(gGHAMap), path)
		return
	}
	runGC()
	err = ioutil.WriteFile(path, bts, 0644)
	if err != nil {
		lib.Printf("cannot write GHA map file %s, %d bytes\n", path, len(bts))
		return
	}
	lib.Printf("saved GHA map %s %d items\n", path, len(gGHAMap))
	return
}

func maxDateGHAMap(ctx *lib.Ctx) *time.Time {
	dt := lib.NDaysStart(time.Now(), cNDaysGHAPeriod)
	for {
		loadGHAMap(ctx, dt)
		if gGHAMap == nil {
			dt = lib.PrevNDaysStart(dt, cNDaysGHAPeriod)
			if dt.Before(gMinGHA) {
				return nil
			}
			continue
		}
		break
	}
	ks := []string{}
	for k := range gGHAMap {
		ks = append(ks, k)
	}
	nKs := len(ks)
	if nKs == 0 {
		return nil
	}
	if nKs > 1 {
		sort.Strings(ks)
	}
	lastKey := ks[len(ks)-1]
	tm, err := time.Parse("2006-01-02-15", lastKey)
	if err != nil {
		return nil
	}
	lib.Printf("max GHA map date is %v\n", tm)
	return &tm
}

func loadGHARepoDates(ctx *lib.Ctx, sha2 string) (ghaRepoDates map[string]map[string]int) {
	if ctx.NoGHARepoDates {
		return
	}
	path := "gha_map/" + sha2 + "_repo_dates.json"
	lib.Printf("loading GHA map repo dates %s\n", path)
	bts, err := ioutil.ReadFile(path)
	if err != nil {
		lib.Printf("cannot read GHA map repo dates file %s\n", path)
		return
	}
	defer func() { runGC() }()
	ghaRepoDates = make(map[string]map[string]int)
	err = jsoniter.Unmarshal(bts, &ghaRepoDates)
	if err != nil {
		lib.Printf("cannot unmarshal from GHA map repo dates file %s, %d bytes\n", path, len(bts))
		return
	}
	nRepos := 0
	for _, repos := range ghaRepoDates {
		nRepos += len(repos)
	}
	lib.Printf("loaded GHA map repo dates %s %d orgs, %d repos\n", path, len(ghaRepoDates), nRepos)
	return
}

func saveGHARepoDates(ctx *lib.Ctx, sha2 string, ghaRepoDates map[string]map[string]int) {
	if ctx.NoGHARepoDates || ghaRepoDates == nil {
		return
	}
	if len(ghaRepoDates) < 1 {
		return
	}
	defer func() { runGC() }()
	nRepos := 0
	for _, repos := range ghaRepoDates {
		nRepos += len(repos)
	}
	path := "gha_map/" + sha2 + "_repo_dates.json"
	lib.Printf("saving GHA map repo dates %s %d orgs, %d items\n", path, len(ghaRepoDates), nRepos)
	runGC()
	bts, err := jsoniter.Marshal(ghaRepoDates)
	if err != nil {
		lib.Printf("cannot marshal GHA map repo dates with %d orgs, %d items to file %s\n", len(ghaRepoDates), nRepos, path)
		return
	}
	runGC()
	err = ioutil.WriteFile(path, bts, 0644)
	if err != nil {
		lib.Printf("cannot write GHA map repo dates file %s, %d bytes\n", path, len(bts))
		return
	}
	lib.Printf("saved GHA map repo dates %s %d orgs, %d items, %d bytes\n", path, len(ghaRepoDates), nRepos, len(bts))
	return
}

func sha2s(s string) string {
	h := sha1.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))[:2]
}

func sha2b(b []byte) string {
	h := sha1.New()
	h.Write(b)
	return hex.EncodeToString(h.Sum(nil))[:2]
}

func updateGHARepoDates(ctx *lib.Ctx) {
	if ctx.NoGHARepoDates {
		return
	}
	defer func() { runGC() }()
	var mtx *sync.Mutex
	thrN := gThrN
	if ctx.MaxParallelSHAs > 0 && thrN > ctx.MaxParallelSHAs {
		thrN = ctx.MaxParallelSHAs
	}
	if thrN > 1 {
		mtx = &sync.Mutex{}
	}
	changedItems, updatedSHAs := 0, 0
	processSHA := func(ch chan struct{}, currSHA string) {
		defer func() {
			if ch != nil {
				ch <- struct{}{}
			}
		}()
		ghaRepoDates := loadGHARepoDates(ctx, currSHA)
		changed := false
		if ghaRepoDates == nil {
			ghaRepoDates = make(map[string]map[string]int)
			changed = false
		}
		lib.Printf("updateGHARepoDates: SHA: %s\n", currSHA)
		for sdt, repos := range gGHAMap {
			dt := lib.ParseGHAString(sdt)
			idt := int(dt.Unix() / int64(3600))
			for r := range repos {
				if sha2s(r) != currSHA {
					continue
				}
				ary := strings.Split(r, "/")
				lAry := len(ary)
				var (
					org  string
					repo string
				)
				if lAry == 1 {
					org = ""
					repo = ary[0]
				} else if lAry == 2 {
					org = ary[0]
					repo = ary[1]
				} else {
					org = ary[0]
					repo = strings.Join(ary[1:], "/")
				}
				orgRepos, ok := ghaRepoDates[org]
				if !ok {
					ghaRepoDates[org] = make(map[string]int)
					ghaRepoDates[org][repo] = idt
					changed = true
					if mtx != nil {
						mtx.Lock()
					}
					changedItems++
					if mtx != nil {
						mtx.Unlock()
					}
					continue
				}
				ridt, ok := orgRepos[repo]
				if ok {
					if idt < ridt {
						ghaRepoDates[org][repo] = idt
						changed = true
						if mtx != nil {
							mtx.Lock()
						}
						changedItems++
						if mtx != nil {
							mtx.Unlock()
						}
						//if currSHA == "f0" {
						//	lib.Printf("%s: SHA: '%s' -> '%s' ('%s','%s',%d->%d)\n", sdt, r, currSHA, org, repo, ridt, idt)
						//}
					}
					continue
				}
				ghaRepoDates[org][repo] = idt
				changed = true
				if mtx != nil {
					mtx.Lock()
				}
				changedItems++
				if mtx != nil {
					mtx.Unlock()
				}
			}
		}
		if changed {
			saveGHARepoDates(ctx, currSHA, ghaRepoDates)
			if mtx != nil {
				mtx.Lock()
			}
			updatedSHAs++
			if mtx != nil {
				mtx.Unlock()
			}
		}
		ghaRepoDates = nil
		runGC()
	}
	if thrN > 1 {
		nThreads := 0
		ch := make(chan struct{})
		for i := 0; i < 0x100; i++ {
			cSHA := fmt.Sprintf("%02x", i)
			go processSHA(ch, cSHA)
			nThreads++
			if nThreads == thrN {
				<-ch
				nThreads--
			}
		}
		for nThreads > 0 {
			<-ch
			nThreads--
		}
	} else {
		for i := 0; i < 0x100; i++ {
			cSHA := fmt.Sprintf("%02x", i)
			processSHA(nil, cSHA)
		}
	}
	lib.Printf("Updated SHAs: %d, items: %d\n", updatedSHAs, changedItems)
}

func handleGHAMap(ctx *lib.Ctx) {
	if ctx.NoGHAMap {
		return
	}
	defer func() { runGC() }()
	maxDt := maxDateGHAMap(ctx)
	if maxDt == nil {
		_ = generateGHAMap(ctx, nil, true, false, true)
	} else {
		nextHour := lib.NextHourStart(*maxDt)
		nextPeriod := lib.NextNDaysStart(*maxDt, cNDaysGHAPeriod)
		if nextHour.Before(nextPeriod) {
			changed := generateGHAMap(ctx, maxDt, false, true, false)
			if changed {
				saveGHAMap(ctx, *maxDt)
			}
		}
		if nextPeriod.Before(time.Now()) {
			_ = generateGHAMap(ctx, maxDt, true, false, true)
		}
	}
}

func handleMT(ctx *lib.Ctx) {
	gThrN = lib.GetThreadsNum(ctx)
	if gThrN > 1 {
		gRichMtx = &sync.Mutex{}
		gEnsuredIndicesMtx = &sync.Mutex{}
		gUploadMtx = &sync.Mutex{}
		gGitHubUsersMtx = &sync.RWMutex{}
		gGitHubMtx = &sync.RWMutex{}
		gIdentityMtx = &sync.Mutex{}
		gUploadDBMtx = &sync.Mutex{}
		gGitHubReposMtx = &sync.RWMutex{}
		gJSONsBytesMtx = &sync.Mutex{}
		gJSONsLockMtx = &sync.Mutex{}
		gJSONsL2Mtx = &sync.Mutex{}
		gSyncAllDatesMtx = &sync.Mutex{}
		dads.SetMT()
	}
}

func getMemUsage() string {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	//return fmt.Sprintf("alloc:%dM heap-alloc:%dM(%dk objs) total:%dM sys:%dM #gc:%d", m.Alloc>>20, m.HeapAlloc>>20, m.HeapObjects>>10, m.TotalAlloc>>20, m.Sys>>20, m.NumGC)
	return fmt.Sprintf("alloc:%dM #gc:%d", m.Alloc>>20, m.NumGC)
}

func runGC() {
	//lib.Printf(getMemUsage() + "\n")
	runtime.GC()
	lib.Printf(getMemUsage() + "\n")
}

func memGCHeartBeat(ctx *lib.Ctx) {
	var m runtime.MemStats
	for {
		time.Sleep(time.Duration(15) * time.Second)
		runtime.GC()
		runtime.ReadMemStats(&m)
		if ctx.MemHeartBeatBytes > 0 && int64(m.Alloc) > ctx.MemHeartBeatBytes {
			lib.Printf("WARNING: mem alloc heartbeat, exceeded limit %dM: %dM\n", ctx.MemHeartBeatBytes>>20, m.Alloc>>20)
		}
	}
}

func initDadsCtx(ctx *lib.Ctx) {
	_ = os.Setenv("DA_DS", "github")
	_ = os.Setenv("DA_GITHUB_DB_CONN", os.Getenv("GHA_DB_CONN"))
	_ = os.Setenv("DA_GITHUB_DB_BULK_SIZE", os.Getenv("GHA_DB_BULK_SIZE"))
	_ = os.Setenv("DA_GITHUB_AFFILIATION_API_URL", os.Getenv("GHA_AFFILIATION_API_URL"))
	gDadsCtx.Init()
	gDadsCtx.Debug = ctx.Debug
	gDadsCtx.ST = ctx.ST
	gDadsCtx.NCPUs = ctx.NCPUs
	gDadsCtx.NCPUsScale = ctx.NCPUsScale
	dads.ConnectAffiliationsDB(&gDadsCtx)
}

func cacheStats() {
	gDadsCtx.Debug = 1
	dads.CacheSummary(&gDadsCtx)
	lib.Printf("docs uploaded: %d\n", gDocsUploaded)
	lib.Printf("indices: %d\n", len(gEnsuredIndices))
	lib.Printf("GitHub API users: %d\n", len(gGitHubUsers))
	lib.Printf("GitHub API repos: %d\n", len(gGitHubRepos))
	lib.Printf("identities uploaded: %d\n", len(gUploadedIdentities))
	lib.Printf("biggest uncompressed JSONs size from a single GHA hour: %dM\n", gMaxJSONsBytes>>20)
}

func main() {
	var ctx lib.Ctx
	dtStart := time.Now()
	debug.SetGCPercent(25)
	ctx.Init()
	initDadsCtx(&ctx)
	path := os.Getenv("GHA_FIXTURES_DIR")
	if len(os.Args) > 1 {
		path = os.Args[1]
	}
	if path == "" {
		lib.Printf("you need to specify fixtures to process either by env variable GHA_FIXTURES_DIR or as an argument\n")
		return
	}
	handleMT(&ctx)
	var (
		config     map[[2]string]*regexp.Regexp
		allRepos   []string
		startDates map[string]map[string]time.Time
	)
	go memGCHeartBeat(&ctx)
	if ctx.LoadConfig {
		var err error
		config, allRepos, err = loadConfigFixtures(&ctx)
		lib.FatalOnError(err)
		startDates, err = loadConfigStartDates(&ctx)
		lib.FatalOnError(err)
	} else {
		config, allRepos = processFixtures(&ctx, lib.GetFixtures(&ctx, path))
		startDates = getStartDates(&ctx, config)
	}
	incremental := handleIncremental(&ctx, config, allRepos, startDates)
	// if !incremental {
	handleGHAMap(&ctx)
	// }
	gha(&ctx, incremental, config, allRepos, startDates)
	dtEnd := time.Now()
	cacheStats()
	lib.Printf("Uploaded: %d, took: %v\n", gDocsUploaded, dtEnd.Sub(dtStart))
}
