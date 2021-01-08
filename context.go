package dadsgha

import (
	"fmt"
	"os"
	"strconv"
)

// Ctx - environment context packed in structure
type Ctx struct {
	Debug             int      // From GHA_DEBUG Debug level: 0-no, 1-info, 2-verbose
	CmdDebug          int      // From GHA_CMDDEBUG Commands execution Debug level: 0-no, 1-only output commands, 2-output commands and their output, 3-output full environment as well, default 0
	ST                bool     // From GHA_ST true: use single threaded version, false: use multi threaded version, default false
	NCPUs             int      // From GHA_NCPUS, set to override number of CPUs to run, this overwrites GHA_ST, default 0 (which means do not use it, use all CPU reported by go library)
	NCPUsScale        float64  // From GHA_NCPUS_SCALE, scale number of CPUs, for example 2.0 will report number of cpus 2.0 the number of actually available CPUs
	ExecFatal         bool     // default true, set this manually to false to avoid lib.ExecCommand calling os.Exit() on failure and return error instead
	ExecQuiet         bool     // default false, set this manually to true to have quiet exec failures
	ExecOutput        bool     // default false, set to true to capture commands STDOUT
	ExecOutputStderr  bool     // default false, set to true to capture commands STDOUT
	GitHubOAuth       string   // From GHA_GITHUB_OAUTH, if not set it attempts to use public access, if contains "/" it will assume that it contains file name, if "," found then it will assume that this is a list of OAuth tokens instead of just one
	ESURL             string   // From GHA_ES_URL - ElasticSearch URL
	ESBulkSize        int      // From GHA_ES_BULK_SIZE, bulk upload size, default 1000
	LoadConfig        bool     // From GHA_LOAD_CONFIG, if set - it will load configuration instead of reading all fixtures
	SaveConfig        bool     // From GHA_SAVE_CONFIG, if set - it will save configuration in a JSON file
	NoIncremental     bool     // From GHA_NO_INCREMENTAL, if set - it will not attempt to detect fixture changes since last run and will treat all fixtures as new and detect the start date everywhere
	NoGHAMap          bool     // From GHA_NO_GHA_MAP, if set - it will not use any GHA map files (which are very memory consuming)
	NoGHARepoDates    bool     // From GHA_NO_GHA_REPO_DATES, if set, it will skip GHA repo dates processing (file is huge, requires around 30G of memory), GHA map files can still be processed
	ConfigFile        string   // From GHA_CONFIG_FILE, configuration save/load file (root name), default "gha_config/" (gha_config/fixtures.json, gha_config/dates.json, gha_config/repos.json)
	GapURL            string   // From GHA_GAP_URL, address of the GAP API
	MaxParallelSHAs   int      // From GHA_MAX_PARALLEL_SHAS, maximum number of GHA repo dates SHA files to process in parallel, setting to 0 means unlimited (basically NCPUS)
	MaxJSONsBytes     int64    // From GHA_MAX_JSONS_GBYTES, when processing multiple GHA hours in parallel, single hour uncompressed can even be 800M, when you have say 64 CPUs then you can reserve 50+ G, so you can specify limit in Gb, default is 0 = no limit
	MemHeartBeatBytes int64    // From GHA_MEM_HEARTBEAT_GBYTES, display memory hearbeat warning when exceeding this limit (heartbeat happens every 15s), default is 0 - not to display any warnings.
	TestMode          bool     // True when running tests
	OAuthKeys         []string // GitHub oauth keys recevide from GHA_GITHUB_OAUTH configuration (initialized only when lib.GHClient() is called)
}

// Init - get context from environment variables
func (ctx *Ctx) Init() {
	ctx.ExecFatal = true
	ctx.ExecQuiet = false
	ctx.ExecOutput = false
	ctx.ExecOutputStderr = false

	// Debug
	if os.Getenv("GHA_DEBUG") == "" {
		ctx.Debug = 0
	} else {
		debugLevel, err := strconv.Atoi(os.Getenv("GHA_DEBUG"))
		FatalOnError(err)
		if debugLevel != 0 {
			ctx.Debug = debugLevel
		}
	}
	// CmdDebug
	if os.Getenv("GHA_CMDDEBUG") == "" {
		ctx.CmdDebug = 0
	} else {
		debugLevel, err := strconv.Atoi(os.Getenv("GHA_CMDDEBUG"))
		FatalOnError(err)
		ctx.CmdDebug = debugLevel
	}

	// Threading
	ctx.ST = os.Getenv("GHA_ST") != ""
	// NCPUs
	if os.Getenv("GHA_NCPUS") == "" {
		ctx.NCPUs = 0
	} else {
		nCPUs, err := strconv.Atoi(os.Getenv("GHA_NCPUS"))
		FatalOnError(err)
		if nCPUs > 0 {
			ctx.NCPUs = nCPUs
			if ctx.NCPUs == 1 {
				ctx.ST = true
			}
		}
	}
	if os.Getenv("GHA_NCPUS_SCALE") == "" {
		ctx.NCPUsScale = 1.0
	} else {
		nCPUsScale, err := strconv.ParseFloat(os.Getenv("GHA_NCPUS_SCALE"), 64)
		FatalOnError(err)
		if nCPUsScale > 0 {
			ctx.NCPUsScale = nCPUsScale
		}
	}

	// Load/Save configuration
	ctx.LoadConfig = os.Getenv("GHA_LOAD_CONFIG") != ""
	ctx.SaveConfig = os.Getenv("GHA_SAVE_CONFIG") != ""
	ctx.ConfigFile = os.Getenv("GHA_CONFIG_FILE")
	if ctx.ConfigFile == "" {
		ctx.ConfigFile = "gha_config/"
	}

	// No incremental mode
	ctx.NoIncremental = os.Getenv("GHA_NO_INCREMENTAL") != ""

	// No GHA map mode
	ctx.NoGHAMap = os.Getenv("GHA_NO_GHA_MAP") != ""
	ctx.NoGHARepoDates = os.Getenv("GHA_NO_GHA_REPO_DATES") != ""

	// GitHub OAuth
	ctx.GitHubOAuth = os.Getenv("GHA_GITHUB_OAUTH")

	// GAP URL API
	ctx.GapURL = os.Getenv("GHA_GAP_URL")

	// ElasticSearch URL
	ctx.ESURL = os.Getenv("GHA_ES_URL")
	if os.Getenv("GHA_ES_BULK_SIZE") == "" {
		ctx.ESBulkSize = 1000
	} else {
		esBulkSize, err := strconv.Atoi(os.Getenv("GHA_ES_BULK_SIZE"))
		FatalOnError(err)
		if esBulkSize > 0 {
			ctx.ESBulkSize = esBulkSize
		}
	}

	// Max parallel SHAs
	if os.Getenv("GHA_MAX_PARALLEL_SHAS") != "" {
		maxParallelSHAs, err := strconv.Atoi(os.Getenv("GHA_MAX_PARALLEL_SHAS"))
		FatalOnError(err)
		if maxParallelSHAs > 0 {
			ctx.MaxParallelSHAs = maxParallelSHAs
		}
	}
	// Max JSONs bytes
	if os.Getenv("GHA_MAX_JSONS_GBYTES") != "" {
		maxJSONsBytes, err := strconv.ParseInt(os.Getenv("GHA_MAX_JSONS_GBYTES"), 10, 64)
		FatalOnError(err)
		if maxJSONsBytes > 0 {
			ctx.MaxJSONsBytes = maxJSONsBytes << 30
		}
	}
	// Memory hearbeat display limit
	if os.Getenv("GHA_MEM_HEARTBEAT_GBYTES") != "" {
		memHeartBeatBytes, err := strconv.ParseInt(os.Getenv("GHA_MEM_HEARTBEAT_GBYTES"), 10, 64)
		FatalOnError(err)
		if memHeartBeatBytes > 0 {
			ctx.MemHeartBeatBytes = memHeartBeatBytes << 30
		}
	}
}

// Print context contents
func (ctx *Ctx) Print() {
	fmt.Printf("Environment Context Dump\n%+v\n", ctx)
}
