from SiemplifyJob import SiemplifyJob

INTEGRATION_NAME = "Custom"
SCRIPT_NAME = "Delete_Playbooks"

def get_playbooks(siemplify, filter_ = [0,1]):
    try:
        res= siemplify.session.post(f"{siemplify.API_ROOT}/external/v1/playbooks/GetWorkflowMenuCardsWithEnvFilter", json= filter_)
        res.raise_for_status()
        return res.json()

    except Exception as e:
        siemplify.LOGGER.error(f"Error getting playbooks: {e}")
        return None

def delete_playbooks (siemplify, playbook_id_list):

    if len(playbook_id_list) > 0:
        
        siemplify.LOGGER.info(str(len(playbook_id_list)) + " playbooks to delete.")

        try:

            res= siemplify.session.post(f"{siemplify.API_ROOT}/external/v1/playbooks/DeleteWorkflows", json={"identifiers": playbook_id_list})
            res.raise_for_status()
            responseJson = res.json()

            if "results" in responseJson.keys():
                for each_element in responseJson["results"]:
                    if "failed" in each_element.keys():
                        if not each_element["failed"]:
                            siemplify.LOGGER.info ("Playbook " + each_element["identifier"] + "  successfully removed")

                        else:
                            siemplify.LOGGER.info ("Playbook " + each_element["identifier"] + " could not be removed")
                            siemplify.LOGGER.info ("Error message: " + each_element["errorMessage"])

            return None

        except Exception as e:
            siemplify.LOGGER.error(f"Error getting playbooks: {e}")
            return None

            
            
def main():
    siemplify = SiemplifyJob()
    siemplify.script_name = SCRIPT_NAME

    


    # INIT ACTION PARAMETERS:
    wl_pb_names = siemplify.extract_job_param(param_name="Playbook Names", print_value=True)

    authotized_creators = ["Siemplify automation", "Andy Shepherd ", "Christopher Martin", "Jose Marin", "Greg Kushmerek", "Michel Oosterhof", "Tarah Lewis", "Jesus Toledano", "Fran Santos", "James Brodsky" ,"Super Admin"] 

    wl_pb_names_temp = []
    wl_pb_names_temp.clear()
    filtered_pbs = []
    filtered_pbs.clear()

    playbooks = get_playbooks(siemplify, filter_=[0])
    pb_blocks = get_playbooks(siemplify, filter_=[1])

    if not playbooks and not pb_blocks:
        siemplify.LOGGER.info("No playbooks found.")
        siemplify.end_script()
        return 

    if wl_pb_names:
        wl_pb_names_temp = wl_pb_names.split(',')
        
    else:
        wl_pb_names_temp = []
    
    siemplify.LOGGER.info(wl_pb_names_temp)

    for pb in playbooks:

        #Check the creator full Name
        if (pb.get("creatorFullName") not in authotized_creators) and "-" not in pb.get("creatorFullName"):
            
            siemplify.LOGGER.info(pb.get("name") + " playbook will be removed")
            filtered_pbs.append(pb.get("identifier"))
        
        #Check the playbooks names
        else:
            if len (wl_pb_names_temp) > 0:
                
                pb_match = False
                
                for each_wl_pb_name in wl_pb_names_temp:

                    if each_wl_pb_name.upper() in pb.get("name").upper() and not pb_match:    
                        pb_match = True          
                
                if not pb_match:
                    siemplify.LOGGER.info(pb.get("name") + " playbook will be removed")
                    filtered_pbs.append(pb.get("identifier"))

    for pb in pb_blocks:

        #Check the creator full Name
        if (pb.get("creatorFullName") not in authotized_creators) and "-" not in pb.get("creatorFullName"):
            
            siemplify.LOGGER.info(pb.get("name") + " block will be removed")
            filtered_pbs.append(pb.get("identifier"))
        
        #Check the playbooks names
        else:
            if len (wl_pb_names_temp) > 0:
                
                pb_match = False
                
                for each_wl_pb_name in wl_pb_names_temp:

                    if each_wl_pb_name.upper() in pb.get("name").upper() and not pb_match:    
                        pb_match = True          
                
                if not pb_match:
                    siemplify.LOGGER.info(pb.get("name") + " block will be removed")
                    filtered_pbs.append(pb.get("identifier"))
    
    if not filtered_pbs:
        siemplify.LOGGER.info("No matching playbooks found.")
        siemplify.end_script()
        return # Important: Add return after siemplify.end_script() to prevent further execution
    
    else:
        delete_playbooks(siemplify, filtered_pbs)

    siemplify.end_script()


if __name__ == "__main__":
    main()