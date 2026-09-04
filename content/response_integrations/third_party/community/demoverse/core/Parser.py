from DataModels import UnstructuredLogs, UdmEvents, DetectionRuleV2, ChronicleRule, ChronicleRuleV2, UnstructuredStreamLogs, UdmStreamEvents


def parse_events(raw_content, file_name, name_space, new_base_time, logstory_timestamps, usecase_mapping=None,
    usecase_name=None, logger=None):
    log_type = file_name.replace('.log','')

    if log_type != "UDM" and name_space == 'LogStory':
        
        return UnstructuredLogs(raw_content, log_type, new_base_time, name_space, logstory_timestamps.get(log_type), usecase_name = usecase_name, logger=logger)
    
    elif log_type != "UDM":

        return UnstructuredLogs(raw_content, log_type, new_base_time, name_space, None, usecase_mapping=usecase_mapping,
            usecase_name = usecase_name, logger = logger)
    
    else:
        
        return UdmEvents(raw_content, new_base_time, name_space, usecase_name, logger=logger)
    

def parse_stream_events(gcs_manager, path, file_name, name_space, new_base_time, logstory_timestamps, usecase_mapping=None, usecase_name=None, start_byte=0, logger=None):
    log_type = file_name.replace('.log','')

    if log_type != "UDM" and name_space == 'LogStory':
        return UnstructuredStreamLogs(gcs_manager, path, log_type, new_base_time, name_space, logstory_timestamps.get(log_type), usecase_name=usecase_name, start_byte=start_byte, logger=logger)
    elif log_type != "UDM":
        return UnstructuredStreamLogs(gcs_manager, path, log_type, new_base_time, name_space, None, usecase_mapping=usecase_mapping, usecase_name=usecase_name, start_byte=start_byte, logger=logger)
    else:
        return UdmStreamEvents(gcs_manager, path, new_base_time, name_space, usecase_name, start_byte=start_byte, logger=logger)
    

def parse_entities(raw_content, file_name, name_space, new_base_time, logstory_timestamps):
    
    log_type = file_name.replace('.log','')
    return UnstructuredLogs(raw_content, log_type, new_base_time, name_space, logstory_timestamps.get(log_type))

def parse_rules_v2(raw_content, file_name):
    
    rule_name = file_name
    
    return DetectionRuleV2(raw_content, rule_name)
    

def parse_chronicle_rules(rules):
    if not rules:
        return None
    return [ChronicleRule(rule) for rule in rules ]

def parse_chronicle_rules_v2(rules):
    if not rules:
        return None
    return [ChronicleRuleV2(rule) for rule in rules ]