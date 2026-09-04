

#CHRONICLE_ENDPOINTS=

EVTX_PATTERNS = {"SystemTime":r"SystemTime=[\"'](.*?)[\"']", "UtcTime":r"UtcTime[\"']\>(.*?)[\"'\<]"}

DEFAULT_REPO_URL = "https://gitlab.com/google-cloud-ce/googlers/josemarin/global-architects-content/secops-demoverse-2.0/demoversedata.git"
DEFAULT_BRANCH = "main"
DEFAULT_BUCKET_NAME = "secops-demoverse-content-2454924198c576e4"
LOCAL_DIR = "/tmp/demoverse-files"
MAX_RETRIES = 40
CHRONICLE_API_URL = "https://malachiteingestion-pa.googleapis.com"
SECOPS_API_URL = "https://{url_prefix}.googleapis.com/v1alpha/{instance}"
API_LIMIT_ERROR = 429
GIT_ENDPOINTS = {
    "get_project_tree": "/api/v4/projects/{project_id}/repository/tree",
    "get_file": "/api/v4/projects/{project_id}/repository/files/{file_path}",
    "create_rule": "/v2/detect/rules",
    "create_rule_version":"v2/detect/rules/{ruleId}:createVersion"
}

DEFAULT_BUCKET_NAME="secops-demoverse-content-2454924198c576e4"

INGESTION_ENDPOINTS = [ 
  "https://northamerica-northeast2-malachiteingestion-pa.googleapis.com",
  "https://me-central2-malachiteingestion-pa.googleapis.com",
  "https://europe-malachiteingestion-pa.googleapis.com",
  "https://europe-west3-malachiteingestion-pa.googleapis.com",
  "https://europe-west2-malachiteingestion-pa.googleapis.com",
  "https://asia-south1-malachiteingestion-pa.googleapis.com",
  "https://asia-southeast1-malachiteingestion-pa.googleapis.com",
  "https://australia-southeast1-malachiteingestion-pa.googleapis.com",
  "https://me-west1-malachiteingestion-pa.googleapis.com",
  "https://asia-northeast1-malachiteingestion-pa.googleapis.com",
  "https://malachiteingestion-pa.googleapis.com",
  "https://europe-west6-malachiteingestion-pa.googleapis.com",

]

GIT_USER = "demoverseUser"
DEFAULT_PROJECT = "49166120"
LOGSTORY_USE_CASES_PATH = "LogStory/usecases"
DEFAULT_USE_CASES_PATH = "{}"
LOGSTORY_RULES_PATH = "LogStory/rules"
LOGSTORY_TIMESTAMPS_FILE_PATH = "LogStory/logstory_timestamps.json"
DEFAULT_RULES_PATH = "{}/rules"
CUSTOM_RULES_PATH = "Custom/rules"
SOAR_CONTENT_PATH = "UseCaseDefinition.json"
DEFAULT_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
TIME_MAP_PATH = "time_map.json"
FOLDER_BLACKLIST =['rules',]

UDMTIMEMAP = {
                "name": "udm_event_timestamp",
                "dateformat": "%Y-%m-%dT%H:%M:%SZ",
                "pattern": '("event_timestamp":\\s*"?)(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z)(\\s*")',
                "epoch": False,
                "group": 2
}
    

DATETIMEMAP = [
    {
        'pattern':'\\d{4}-\\d{2}-\\d{2}[T\\s]{1}\\d{2}:\\d{2}:\\d{2}Z?',
        'formats':[
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d %H:%M:%S'
        ]
    },
    {
        'pattern':'\\d{4}\\d{2}\\d{2}T\\d{2}\\d{2}\\d{2}',
        'formats':[
            '%Y%m%dT%H%M%S'
        ]
    },
    {
        'pattern':'\\d{4}-\\d{2}-\\d{2}(?![T\\s\\d\\-:]{1})(?!\\d{2}:\\d{2}:\\d{2}Z?)',
        'formats':[
            '%Y-%m-%d'
        ]
    },
    {
        'pattern':'\\d{1,2}\\/\\d{1,2}\\/\\d{2}[,\\s]{1}\\d{1,2}:\\d{1,2}:\\d{1,2}',
        'formats':[
            '%m/%d/%y,%H:%M:%S',
            '%m/%d/%y %H:%M:%S'
        ]
    },
    {
        'pattern':'\\d+/\\d+/\\d{4} \\d+:\\d+:\\d+ [AP]M',
        'formats':[
            '%m/%d/%Y %I:%M:%S %p'
        ]
    },
    {
        'pattern':'[a-zA-Z]{3}\\s+\\d+\\s+\\d\\d:\\d\\d:\\d\\d',
        'formats':[
            '%b %d %H:%M:%S'
        ]
    },
    {
        'pattern':'[a-zA-Z]{3}\\s+\\d{1,2}\\s+\\d{4}\\s+\\d{2}:\\d{2}:\\d{2}',
        'formats':[
            '%b %d %Y %H:%M:%S'
        ]
    },
    {
        'pattern':'\\d{4}\\s+[a-zA-Z]{3}\\s+\\d{1,2}\\s+\\d{2}:\\d{2}:\\d{2}',
        'formats':[
            '%Y %b %d %H:%M:%S'
        ]
    },
    {
        'pattern':'\\d{1,2}\\/[a-zA-Z]{3}\\/\\d{4}:\\d\\d:\\d\\d:\\d\\d',
        'formats':[
            '%d/%b/%Y:%H:%M:%S'
        ]
    },
    {
        'pattern':'[a-zA-Z]{3}\\s\\d+,\\s\\d{4}\\s+\\d+:\\d+:\\d+\\s[AP]M',
        'formats':[
            '%b %d, %Y %H:%M:%S %p'
        ]
    },
    {
        'pattern':'date=\\d{4}-\\d{2}-\\d{2}\\stime=\\d{2}:\\d{2}:\\d{2}',
        'formats':[
            'date=%Y-%m-%d time=%H:%M:%S'
        ]
    },
    {
        'pattern':'(?<![A-Za-z\\d\-@])\\d{10}(?![A-Za-z\\d\-@])',
        'formats':[
            '%s'
        ]
    },
    {
        'pattern':'(?<![A-Za-z\\d\-@])\\d{13}(?![A-Za-z\\d\-@])',
        'formats':[
            '%ms'
        ]
    }
]

SEARCH_CASE_PAYLOAD = {
  "tags": [],
  "ruleGenerator": [],
  "caseSource": [],
  "stage": [],
  "environments": [],
  "assignedUsers": [],
  "products": [],
  "ports": [],
  "categoryOutcomes": [],
  "status": [],
  "caseIds": [],
  "incident": [],
  "importance": [],
  "priorities": [],
  "pageSize": 100,
  "isCaseClosed": False,
  "title": "",
  "startTime": None,
  "endTime": None,
  "requestedPage": 0,
  "timeRangeFilter": 0
}

SEARCH_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

STREAM_TIME_UNITS = {
    "h": "hours",
    "m": "minutes",
    "s": "seconds"
}