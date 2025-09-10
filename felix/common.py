import json
import logging
import logging.config
import os
import os.path

from oci.core import ComputeClient
from oci.identity import IdentityClient
from oci.work_requests import WorkRequestClient
import oci
from .config import region

REPAIR_KEY = 'repairDetails'
FAULT_KEY = 'faultDetails'
IPADDRESS_KEY = 'primaryVnicAddress'


def _apply_region(client):
    """Ensure region is always applied from config.py"""
    client.base_client.set_region(region)
    return client

def make_compute_client(**kwargs):
    signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    retry = kwargs.pop(
        "retry_strategy",
        oci.retry.RetryStrategyBuilder(max_attempts=60).get_retry_strategy()
    )
    client = ComputeClient({}, signer=signer, retry_strategy=retry, **kwargs)
    return _apply_region(client)

def make_workrequests_client(**kwargs):
    signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    client = WorkRequestClient({}, signer=signer, **kwargs)
    return _apply_region(client)

def make_identity_client(**kwargs):
    signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
    client = IdentityClient({}, signer=signer, **kwargs)
    return _apply_region(client)


def setup_logging(default_path='logging.json', default_level=logging.INFO, env_key='LOG_CFG_FILEPATH'):
    """Setup a json-based logging configuration
    """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'r') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


#def make_compute_client(**kwargs):
#    if use_instance_principal:
#        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
#        client = ComputeClient(dict(), retry_strategy=oci.retry.RetryStrategyBuilder(max_attempts=60).get_retry_strategy(),  signer=signer, **kwargs)
#    else:
#        config = oci.config.from_file('~/.oci/config', profile_name='DEFAULT')
#        client = ComputeClient(config, **kwargs)
#    return client

#def make_workrequests_client(**kwargs):
#    if use_instance_principal:
#        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
#        client = WorkRequestClient(dict(), signer=signer, **kwargs)
#    else:
#        config = oci.config.from_file('~/.oci/config', profile_name='DEFAULT')
#        client = WorkRequestClient(config, **kwargs)
#    return client

#def make_identity_client(**kwargs):
#    if use_instance_principal:
#        signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
#        client = IdentityClient(dict(), signer=signer, **kwargs)
#    else:
#        config = oci.config.from_file('~/.oci/config', profile_name='DEFAULT')
#        client = IdentityClient(config, **kwargs)
#    return client


def is_workrequest_terminal(response):
    """Determine if the WorkRequest is complete"""
    return response.data in ['SUCCESSFUL', 'FAILED', 'CANCELED']


def paginated_api_to_generator(api_invocation, *args, **kwargs):
    """A utility function for interacting with paginated list APIs. This transforms any paginated list API into a Python
    generator.
    :arg api_invocation a function reference to the API to be called
    :arg *args any arguments to the function
    :arg **kwargs any named arguments to the function
    """
    next_page = None
    while True:
        response = api_invocation(*args, page=next_page, **kwargs)
        assert isinstance(response, oci.base_client.Response)
        for item in response.data:
            yield item

        if response.has_next_page:
            next_page = response.next_page
        else:
            break


def get_additional_details(additional_details):
    """Retrieve the additionalDetails data in a more usable format

    :returns primary_vnic_address as a string, repair_details as list of Repair, fault_details as list of Fault

    """
    if additional_details is None:
        additional_details = dict()

    repair_details = additional_details.get(REPAIR_KEY, list())
    repair_details = _json_string_to_object(repair_details)
    repair_details = [Repair(repair) for repair in repair_details]

    fault_details = additional_details.get(FAULT_KEY, list())
    fault_details = _json_string_to_object(fault_details)
    fault_details = [Fault(fault) for fault in fault_details]

    primary_vnic_address = additional_details.get(IPADDRESS_KEY)

    return primary_vnic_address, repair_details, fault_details


def _json_string_to_object(json_str):
    """Converts a json string to a json tree (if it is a string, or returns it as-is)"""
    if isinstance(json_str, str):
        return json.loads(json_str)
    return json_str


class Fault(object):
    def __init__(self, fault_dict):
        self.customer_description = fault_dict['customerDescription']
        self.component = fault_dict['faultComponent']
        self.fault_id = fault_dict['faultId']
        self.impact_description = fault_dict['impactDescription']
        self.impact_type = fault_dict['impactType']
        self.recommended_action = fault_dict['recommendedAction']

    def __str__(self):
        return f"Fault for component {self.component} with uniqueId {self.fault_id}."


class Repair(object):
    def __init__(self, repair_dict):
        self.component_identifier = repair_dict['componentIdentifier']
        self.component_type = repair_dict['componentType']
        self.repair_type = repair_dict['repairType']

    def __str__(self):
        return f"Repair for {self.component_type} of type {self.repair_type} for {self.component_identifier}."
