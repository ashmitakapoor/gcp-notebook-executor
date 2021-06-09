import base64
from googleapiclient import discovery
import pytz
import datetime

#-------------------- Configurations --------------------
#GCP_PROJECT = "ainotebooktest"
#GCS_BUCKET_PATH_INPUT = "gs://ainotebook-bucket"
#GCS_BUCKET_PATH_OUTPUT = "gs://ainotebook-bucket/rfolder"
STARTUP_SCRIPT_URL = "https://raw.githubusercontent.com/rishisinghal/gcp-notebook-executor/master/notebook_executor.sh"

#PROJECT_NAME = "ainotebooktest"
NOTEBOOK_NAME = "rsample.ipynb"
DLVM_IMAGE_PROJECT = "deeplearning-platform-release"
DLVM_IMAGE_FAMILY = "r-3-6-cpu-experimental-notebooks-debian-10" 
#ZONE = "us-west1-b"
#MACHINE_TYPE = "n1-standard-4"
#MACHINE_NAME = "ainotebooktest-mn"
BOOT_DISK_SIZE = "200GB"
#GPU_TYPE = "nvidia-tesla-k80"
#GPU_COUNT = 1
INSTALL_NVIDIA_DRIVER = False


def create_instance(GCP_PROJECT,GCS_BUCKET_PATH_INPUT,GCS_BUCKET_PATH_OUTPUT,ZONE,MACHINE_TYPE,MACHINE_NAME):

    # Create the Cloud Compute Engine service object
    compute = discovery.build('compute', 'v1')
    
    image_response = compute.images().getFromFamily(
        project=DLVM_IMAGE_PROJECT, family=DLVM_IMAGE_FAMILY).execute()
    source_disk_image = image_response['selfLink']

    # Configure the machine
    machine_type_with_zone = "zones/%s/machineTypes/%s" % (ZONE,MACHINE_TYPE)
    
    today_date = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))

    # Following are my changes 
    gcs_input_notebook = "%s/%s" % (GCS_BUCKET_PATH_INPUT,NOTEBOOK_NAME)
    gcs_output_folder = "%s/outputs/%s/%s/%s/" % (GCS_BUCKET_PATH_OUTPUT,today_date.year,today_date.month,today_date.day)
    gcs_parameters_file= "%s/%s" % (GCS_BUCKET_PATH_INPUT,"params.yaml")
    gcs_requirements_txt= "%s/%s" % (GCS_BUCKET_PATH_INPUT,"requirements.txt")

    #accelerator_type = "projects/%s/zones/%s/acceleratorTypes/%s" % (GCP_PROJECT,ZONE,GPU_TYPE)

    config = {
        'name': MACHINE_NAME,
        'machineType': machine_type_with_zone,

        # Specify the boot disk and the image to use as a source.
        'disks': [
            {
                'boot': True,
                'autoDelete': True,
                'initializeParams': {
                    'sourceImage': source_disk_image,
                },
                'boot-disk-size': BOOT_DISK_SIZE
            }
        ],
        
        # Specify a network interface with NAT to access the public
        # internet.
        'networkInterfaces': [{
            'network': 'global/networks/default',
            'accessConfigs': [
                {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
            ]
        }],

        #'guestAccelerators': [{
        #    'acceleratorType':accelerator_type,
        #    'acceleratorCount':GPU_COUNT
        #}],

        'scheduling': {
            'onHostMaintenance': 'TERMINATE'
        },

        # Allow the instance to access cloud storage and logging.
        'serviceAccounts': [{
            'email': 'default',
            'scopes': [
                'https://www.googleapis.com/auth/cloud-platform'
            ]
        }],

        # Metadata is readable from the instance and allows you to
        # pass configuration from deployment scripts to instances.
        'metadata': {
            'items': [{
                # Startup script is automatically executed by the
                # instance upon startup.
                'key': 'startup-script-url',
                'value': STARTUP_SCRIPT_URL
            }, {
                'key': 'input_notebook',
                'value': gcs_input_notebook
            }, {
                'key': 'output_notebook',
                'value': gcs_output_folder
            }, {
                'key': 'requirements_txt',
                'value': gcs_requirements_txt 
            }, {
                'key': 'parameters_file',
                'value': gcs_parameters_file
            }, {
                'key': 'install-nvidia-driver',
                'value': INSTALL_NVIDIA_DRIVER
            }]
        }
    }

    return compute.instances().insert(
        project=GCP_PROJECT,
        zone=ZONE,
        body=config).execute()
        
        
def execute(event,context):
    #print("""This Function was triggered by messageId {} published at {} """.format(context.event_id, context.timestamp))
    projectId = event['attributes']['projectId']
    bucketInput = event['attributes']['bucketInput']
    bucketOutput = event['attributes']['bucketOutput']
    zone = event['attributes']['zone']
    machineType = event['attributes']['machineType']
    machineName = event['attributes']['machineName']
    resp = create_instance(
        GCP_PROJECT=projectId,
        GCS_BUCKET_PATH_INPUT=bucketInput,
        GCS_BUCKET_PATH_OUTPUT=bucketOutput,
        ZONE=zone,
        MACHINE_TYPE=machineType,MACHINE_NAME=machineName)
    print("Response",str(resp))
    return str(resp)
