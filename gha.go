package dadsgha

import (
	"fmt"
	"time"
)

// Event - full GHA (GitHub Archive) event structure
type Event struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Public    bool      `json:"public"`
	CreatedAt time.Time `json:"created_at"`
	Actor     Actor     `json:"actor"`
	Repo      Repo      `json:"repo"`
	Org       *Org      `json:"org"`
	Payload   Payload   `json:"payload"`
	// Extra fields added by dadsgha
	GHADt      time.Time         `json:"gha_hour"`
	GHAFxSlug  string            `json:"gha_fixture_slug"`
	GHAProj    string            `json:"gha_project"`
	GHASuffMap map[string]string `json:"gha_suff_map"`
}

// EventOld - full GHA (GitHub Archive) event structure, before 2015
type EventOld struct {
	ID         string      `json:"-"`
	Type       string      `json:"type"`
	Public     bool        `json:"public"`
	CreatedAt  time.Time   `json:"created_at"`
	Actor      string      `json:"actor"`
	Repository ForkeeOld   `json:"repository"`
	Payload    *PayloadOld `json:"payload"`
	// Extra fields added by dadsgha
	GHADt      time.Time         `json:"gha_hour"`
	GHAFxSlug  string            `json:"gha_fixture_slug"`
	GHAProj    string            `json:"gha_project"`
	GHASuffMap map[string]string `json:"gha_suff_map"`
}

// Payload - GHA Payload structure
type Payload struct {
	PushID       *int         `json:"push_id"`
	Size         *int         `json:"size"`
	Ref          *string      `json:"ref"`
	Head         *string      `json:"head"`
	Before       *string      `json:"before"`
	Action       *string      `json:"action"`
	RefType      *string      `json:"ref_type"`
	MasterBranch *string      `json:"master_branch"`
	Description  *string      `json:"description"`
	Number       *int         `json:"number"`
	Forkee       *Forkee      `json:"forkee"`
	Release      *Release     `json:"release"`
	Member       *Actor       `json:"member"`
	Issue        *Issue       `json:"issue"`
	Comment      *Comment     `json:"comment"`
	Commits      *[]Commit    `json:"commits"`
	Pages        *[]Page      `json:"pages"`
	PullRequest  *PullRequest `json:"pull_request"`
}

// PayloadOld - GHA Payload structure (from before 2015)
type PayloadOld struct {
	Issue        *int           `json:"issue"`
	IssueID      *int           `json:"issue_id"`
	Comment      *Comment       `json:"comment"`
	CommentID    *int           `json:"comment_id"`
	Description  *string        `json:"description"`
	MasterBranch *string        `json:"master_branch"`
	Ref          *string        `json:"ref"`
	Action       *string        `json:"action"`
	RefType      *string        `json:"ref_type"`
	Head         *string        `json:"head"`
	Size         *int           `json:"size"`
	Number       *int           `json:"number"`
	PullRequest  *PullRequest   `json:"pull_request"`
	Member       *Actor         `json:"member"`
	Release      *Release       `json:"release"`
	Pages        *[]Page        `json:"pages"`
	Commit       *string        `json:"commit"`
	SHAs         *[]interface{} `json:"shas"`
	Repository   *Forkee        `json:"repository"`
	Team         *Team          `json:"team"`
}

// ForkeeOld - GHA Forkee structure (from before 2015)
// Handle missing 4 last properties (including two non-nulls!)
type ForkeeOld struct {
	ID            int        `json:"id"`
	CreatedAt     time.Time  `json:"created_at"`
	Description   *string    `json:"description"`
	Fork          bool       `json:"fork"`
	Forks         int        `json:"forks"`
	HasDownloads  bool       `json:"has_downloads"`
	HasIssues     bool       `json:"has_issues"`
	HasWiki       bool       `json:"has_wiki"`
	Homepage      *string    `json:"homepage"`
	Language      *string    `json:"language"`
	DefaultBranch string     `json:"master_branch"`
	Name          string     `json:"name"`
	OpenIssues    int        `json:"open_issues"`
	Organization  *string    `json:"organization"`
	Owner         string     `json:"owner"`
	Private       *bool      `json:"private"`
	PushedAt      *time.Time `json:"pushed_at"`
	Size          int        `json:"size"`
	Stargazers    int        `json:"stargazers"`
	Watchers      int        `json:"watchers"`
}

// Repo - GHA Repo structure
type Repo struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// Actor - GHA Actor structure
// Name is unexported and not used by JSON load/save
// But is used when importing affiliations from cncf/gitdm:github_users.json
type Actor struct {
	ID    int    `json:"id"`
	Login string `json:"login"`
	Name  string `json:"-"`
}

// Org - GHA Org structure
type Org struct {
	ID    int    `json:"id"`
	Login string `json:"login"`
}

// Forkee - GHA Forkee structure
type Forkee struct {
	ID              int        `json:"id"`
	Name            string     `json:"name"`
	FullName        string     `json:"full_name"`
	Owner           Actor      `json:"owner"`
	Description     *string    `json:"description"`
	Public          *bool      `json:"public"`
	Fork            bool       `json:"fork"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
	PushedAt        *time.Time `json:"pushed_at"`
	Homepage        *string    `json:"homepage"`
	Size            int        `json:"size"`
	StargazersCount int        `json:"stargazers_count"`
	HasIssues       bool       `json:"has_issues"`
	HasProjects     *bool      `json:"has_projects"`
	HasDownloads    bool       `json:"has_downloads"`
	HasWiki         bool       `json:"has_wiki"`
	HasPages        *bool      `json:"has_pages"`
	Forks           int        `json:"forks"`
	OpenIssues      int        `json:"open_issues"`
	Watchers        int        `json:"watchers"`
	DefaultBranch   string     `json:"default_branch"`
}

// Release - GHA Release structure
type Release struct {
	ID              int        `json:"id"`
	TagName         string     `json:"tag_name"`
	TargetCommitish string     `json:"target_commitish"`
	Name            *string    `json:"name"`
	Draft           bool       `json:"draft"`
	Author          Actor      `json:"author"`
	Prerelease      bool       `json:"prerelease"`
	CreatedAt       time.Time  `json:"created_at"`
	PublishedAt     *time.Time `json:"published_at"`
	Body            *string    `json:"body"`
	Assets          []Asset    `json:"assets"`
}

// Asset - GHA Asset structure
type Asset struct {
	ID            int       `json:"id"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
	Name          string    `json:"name"`
	Label         *string   `json:"label"`
	Uploader      Actor     `json:"uploader"`
	ContentType   string    `json:"content_type"`
	State         string    `json:"state"`
	Size          int       `json:"size"`
	DownloadCount int       `json:"download_count"`
}

// PullRequest - GHA Pull Request structure
type PullRequest struct {
	ID                  int        `json:"id"`
	Base                Branch     `json:"base"`
	Head                Branch     `json:"head"`
	User                Actor      `json:"user"`
	Number              int        `json:"number"`
	State               string     `json:"state"`
	Locked              *bool      `json:"locked"`
	Title               string     `json:"title"`
	Body                *string    `json:"body"`
	CreatedAt           time.Time  `json:"created_at"`
	UpdatedAt           time.Time  `json:"updated_at"`
	ClosedAt            *time.Time `json:"closed_at"`
	MergedAt            *time.Time `json:"merged_at"`
	MergeCommitSHA      *string    `json:"merge_commit_sha"`
	Assignee            *Actor     `json:"assignee"`
	Assignees           *[]Actor   `json:"assignees"`
	RequestedReviewers  *[]Actor   `json:"requested_reviewers"`
	Milestone           *Milestone `json:"milestone"`
	Merged              *bool      `json:"merged"`
	Mergeable           *bool      `json:"mergeable"`
	MergedBy            *Actor     `json:"merged_by"`
	MergeableState      *string    `json:"mergeable_state"`
	Rebaseable          *bool      `json:"rebaseable"`
	Comments            *int       `json:"comments"`
	ReviewComments      *int       `json:"review_comments"`
	MaintainerCanModify *bool      `json:"maintainer_can_modify"`
	Commits             *int       `json:"commits"`
	Additions           *int       `json:"additions"`
	Deletions           *int       `json:"deletions"`
	ChangedFiles        *int       `json:"changed_files"`
}

// Branch - GHA Branch structure
type Branch struct {
	SHA   string  `json:"sha"`
	User  *Actor  `json:"user"`
	Repo  *Forkee `json:"repo"` // This is confusing, but actually GHA has "repo" fields that holds "forkee" structure
	Label string  `json:"label"`
	Ref   string  `json:"ref"`
}

// Issue - GHA Issue structure
type Issue struct {
	ID          int        `json:"id"`
	Number      int        `json:"number"`
	Comments    int        `json:"comments"`
	Title       string     `json:"title"`
	State       string     `json:"state"`
	Locked      bool       `json:"locked"`
	Body        *string    `json:"body"`
	User        Actor      `json:"user"`
	Assignee    *Actor     `json:"assignee"`
	Labels      []Label    `json:"labels"`
	Assignees   []Actor    `json:"assignees"`
	Milestone   *Milestone `json:"milestone"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	ClosedAt    *time.Time `json:"closed_at"`
	PullRequest *struct{}  `json:"pull_request"`
}

// Label - GHA Label structure
type Label struct {
	ID      *int   `json:"id"`
	Name    string `json:"name"`
	Color   string `json:"color"`
	Default *bool  `json:"default"`
}

// Milestone - GHA Milestone structure
type Milestone struct {
	ID           int        `json:"id"`
	Name         string     `json:"name"`
	Number       int        `json:"number"`
	Title        string     `json:"title"`
	Description  *string    `json:"description"`
	Creator      *Actor     `json:"creator"`
	OpenIssues   int        `json:"open_issues"`
	ClosedIssues int        `json:"closed_issues"`
	State        string     `json:"state"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
	ClosedAt     *time.Time `json:"closed_at"`
	DueOn        *time.Time `json:"due_on"`
}

// Comment - GHA Comment structure
type Comment struct {
	ID                  int       `json:"id"`
	Body                string    `json:"body"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
	User                Actor     `json:"user"`
	CommitID            *string   `json:"commit_id"`
	OriginalCommitID    *string   `json:"original_commit_id"`
	DiffHunk            *string   `json:"diff_hunk"`
	Position            *int      `json:"position"`
	OriginalPosition    *int      `json:"original_position"`
	Path                *string   `json:"path"`
	PullRequestReviewID *int      `json:"pull_request_review_id"`
	Line                *int      `json:"line"`
}

// Commit - GHA Commit structure
type Commit struct {
	SHA      string `json:"sha"`
	Author   Author `json:"author"`
	Message  string `json:"message"`
	Distinct bool   `json:"distinct"`
}

// Author - GHA Commit Author structure
type Author struct {
	Name  string `json:"name"`
	Email string `json:"email"`
}

// Page - GHA Page structure
type Page struct {
	SHA    string `json:"sha"`
	Action string `json:"action"`
	Title  string `json:"title"`
}

// Team - GHA Team structure (only used before 2015)
type Team struct {
	ID         int    `json:"id"`
	Name       string `json:"name"`
	Slug       string `json:"slug"`
	Permission string `json:"permission"`
}

// MakeOldRepoName - before 2015 repository name should be Organization/Name (if Organization present) or just Name
func MakeOldRepoName(repo *ForkeeOld) string {
	if repo.Organization == nil || *repo.Organization == "" {
		return repo.Name
	}
	// FIXME: what if repo.NAME == "" --> "kubernetes/"
	return fmt.Sprintf("%s/%s", *repo.Organization, repo.Name)
}
