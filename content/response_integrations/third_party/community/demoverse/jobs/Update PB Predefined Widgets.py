from SiemplifyJob import SiemplifyJob
import copy

INTEGRATION_NAME = "CUSTOM_INTEGRATION"
SCRIPT_NAME = "JobTemplate"

def get_playbooks(siemplify, filter_ = [0,1]):
    try:
        res= siemplify.session.post(f"{siemplify.API_ROOT}/external/v1/playbooks/GetWorkflowMenuCardsWithEnvFilter", json= filter_)
        res.raise_for_status()
        return res.json()
    except Exception as e:
        siemplify.LOGGER.error(f"Error getting playbooks: {e}")
        return None

def get_pb_details(siemplify, pb_id):
    try:
        res = siemplify.session.get(f"{siemplify.API_ROOT}/external/v1/playbooks/GetWorkflowFullInfoByIdentifier/{pb_id}")
        res.raise_for_status()
        return res.json()
    except Exception as e:
        siemplify.LOGGER.error(f"Error getting playbook details: {e}")
        return None

def get_action_templates(siemplify, actions):
    params = {"actionIdentifiers": actions}
    try:
        res = siemplify.session.get(f"{siemplify.API_ROOT}/external/v1/playbooks/action-widget-template", params=params)
        res.raise_for_status()
        return res.json()
    except Exception as e:
        siemplify.LOGGER.error(f"Error getting action templates: {e}")
        return None

def update_playbook(siemplify, pb_id, pb_details):
    try:
        res = siemplify.session.post(f"{siemplify.API_ROOT}/external/v1/playbooks/SaveWorkflowDefinitions", json=pb_details)
        res.raise_for_status()
        siemplify.LOGGER.info(f"Playbook {pb_id} updated successfully!")
        return True
    except Exception as e:
        siemplify.LOGGER.error(f"Error updating playbook: {e}")
        return False

def main():
    siemplify = SiemplifyJob()
    siemplify.script_name = SCRIPT_NAME

    


    # INIT ACTION PARAMETERS:
    relevant_folders = siemplify.extract_job_param(param_name="Playbook Folders", print_value=True) # Replace with your actual param name


    playbooks = get_playbooks(siemplify, filter_=[0])
    blocks = get_playbooks(siemplify, filter_=[1])

    if relevant_folders:
        relevant_folders = relevant_folders.split(',')
    else:
        relevant_folders = []

    if not playbooks:
        siemplify.LOGGER.info("No playbooks found.")
        siemplify.end_script()
        return # Important: Add return after siemplify.end_script() to prevent further execution


    filtered_pbs = [pb for pb in playbooks if pb.get("categoryName") in ["Blocks", "Cymbal", "LogStory"] + relevant_folders] # Modified slicing

    if not filtered_pbs:
        siemplify.LOGGER.info("No matching playbooks found.")
        siemplify.end_script()
        return # Important: Add return after siemplify.end_script() to prevent further execution

    for pb in filtered_pbs:
        pb_details = get_pb_details(siemplify, pb["identifier"])
        if pb_details is None:
            siemplify.LOGGER.error(f"Failed to get details for playbook {pb['name']}. Skipping.")
            continue

        updated_views = []
        for view in pb_details.get("overviewTemplates",): # Fixed missing
            predefined_widgets = []
            for widget in view.get("widgets",): # Fixed missing
                if widget.get('metadata', {}).get("predefinedWidgetTemplateIdentifier"):
                    predefined_widgets.append(widget)

            action_names = set()
            for widget in predefined_widgets:
                for step in pb_details.get('steps',): # Fixed missing
                    if step.get('identifier') == widget.get('metadata', {}).get('stepIdentifier'):
                        widget['metadata']['actionName'] = step.get('actionName')
                        action_names.add(step.get('actionName'))

                    if step.get('actionName') == "NestedAction":
                        for block in blocks:
                            if block is None:
                                continue
                            if block.get('name') == step.get('name'):
                                block_details = get_pb_details(siemplify, block.get('identifier'))
                                if block_details is None:
                                    continue
                                for b_step in block_details.get('steps',): # Fixed missing
                                    if b_step.get('identifier') == widget.get('metadata', {}).get('stepIdentifier'):
                                        widget['metadata']['actionName'] = b_step.get('actionName')
                                        action_names.add(b_step.get('actionName'))

            action_templates = get_action_templates(siemplify, list(action_names))

            if action_templates is None:
                continue

            updated_widgets = []
            widgets_to_remove = []
            for widget in predefined_widgets:
                for template in action_templates:
                    if template.get('metadata', {}).get('actionIdentifier') == widget.get('metadata', {}).get('actionName'):
                        if template.get('config', {}).get('htmlContent')!= widget.get('config', {}).get('htmlContent'):
                            new_widget = copy.deepcopy(widget)
                            new_widget['config']['htmlContent'] = template.get('config', {}).get('htmlContent')
                            new_widget['metadata']['predefinedWidgetTemplateIdentifier'] = template.get('metadata', {}).get('predefinedWidgetTemplateIdentifier')
                            updated_widgets.append(new_widget)
                            widgets_to_remove.append(widget)
                            break

            if not updated_widgets:
                continue

            new_view = copy.deepcopy(view)
            original_widgets = new_view.get('widgets',) # Fixed missing

            for widget_to_remove in widgets_to_remove:
                if widget_to_remove in original_widgets:
                    original_widgets.remove(widget_to_remove)

            new_view['widgets'] = original_widgets + updated_widgets
            updated_views.append(new_view)

        if updated_views:
            pb_details["overviewTemplates"] = updated_views

            if update_playbook(siemplify, pb["identifier"], pb_details):
                siemplify.LOGGER.info(f"Playbook {pb['name']} updated successfully!")
            else:
                siemplify.LOGGER.error(f"Failed to update playbook {pb['name']}.")
        else:
            siemplify.LOGGER.info(f"No updates needed for playbook {pb['name']}.")

    siemplify.end_script()


if __name__ == "__main__":
    main()