import os
import re
import zipfile
import shutil
import Evtx.Evtx as evtx  #!pip3 install python-evtx
from constants import EVTX_PATTERNS
from datetime import datetime, timedelta


def delete_legacy_files(path):
    shutil.rmtree(path)

def extract_zip(source, dst):
    with zipfile.ZipFile(source, 'r') as zip_ref:
        zip_ref.extractall(f"{dst}/zip_files")

    return f"{dst}/zip_files"

def is_file_empty(filename):
  """Checks if a file is empty.

  Args:
    filename: The path to the file.

  Returns:
    True if the file is empty, False otherwise.
  """

  return os.stat(filename).st_size == 0

def read_file(filename):
  """
  This function reads a file and returns its content as a string.

  Args:
      filename: The path to the file you want to read.

  Returns:
      A string containing the contents of the file, or None if there's an error.
  """

  try:
    with open(filename, "r") as file:
        content = "".join(
            line.replace("\t", "\\t") for line in file if line.strip()
        )           
        return content      
  except FileNotFoundError as e:
    raise ValueError(f"Error: File not found - {filename}-{e}")
    return None
  except Exception as e:
    raise ValueError(f"Error reading file {filename}: {e}")
    return None
  

def find_regex_matches(text, pattern):
  """
  This function finds matches in a string using a regex pattern and returns the match groups.

  Args:
      text: The string to search for matches.
      pattern: The regular expression pattern to use.

  Returns:
      A list of match groups found in the string, or an empty list if no matches are found.
  """

  regex = re.compile(pattern, re.DOTALL)

  matches = regex.findall(text)

  return matches



def extract_xml_from_evtx(evtx_file_path, output_file_path):
    """Extracts XML data from an EVTX file and saves it to an XML file.

    Args:
        evtx_file_path (str): The path to the input EVTX file.
        output_file_path (str): The path to the output XML file.

    Raises:
        FileNotFoundError: If the input EVTX file does not exist.
        TypeError: If the input path is not a file.
        PermissionError: If the input EVTX file is not readable.
    """  
    with evtx.Evtx(evtx_file_path) as log:
        with open(output_file_path, 'w', encoding='utf-8') as outfile:
            outfile.write('<Events>\n')  # Start the XML document
            for record in log.records():
                try:
                    outfile.write(record.xml().replace('\n', '') + '\n')
                except Exception as e:
                    raise ValueError(f"Error processing record: {e}")
            outfile.write('</Events>\n')  # Close the XML document




def check_evtx_type(evtx):
    """
    Checks a string for specific identifiers and returns a corresponding type.

    Args:
        text: The string to check.

    Returns:
        The detected Chronicle Ingestion Label based upon the Channel
        Returns WINEVTLOG if no exact match is found.
    """   

    # Security
    if "Microsoft-Windows-Security-Auditing" in evtx:
        return "WINEVTLOG"
    # System
    elif "<Channel>System</Channel>" in evtx:
        return "WINEVTLOG"       
    # Application
    elif "<Channel>Application</Channel>" in evtx:
       return "WINEVTLOG"
    # Setup
    elif "<Channel>Setup</Channel>" in evtx:
       return "WINEVTLOG"       
    # Sysmon
    elif "Microsoft-Windows-Sysmon/Operational" in evtx:
       return "WINDOWS_SYSMON"
    # PowerShell 
    elif "<Channel>PowerShellCore/Operational</Channel>" in evtx:
        return "POWERSHELL"
    elif "<Channel>Windows PowerShell</Channel>" in evtx:
        return "POWERSHELL"
    elif "<Channel>Microsoft-Windows-PowerShell/Operational</Channel>" in evtx:
        return "POWERSHELL"
    #linux Sysmon
    elif "Linux-Sysmon" in evtx:
        return "LINUX_SYSMON"       
    # Catch-all            
    else:
        return "WINEVTLOG"

def extract_datetime(log, pattern):
    """Extracts the datetime object from a log string."""
    match = re.search(EVTX_PATTERNS.get(pattern), log)
    if match:
        datetime_str = match.group(1)
        datetime_str_parsed = re.sub(r'(\.\d{6})\d+', r'\1', datetime_str)
        datetime_str_parsed = f"{datetime_str_parsed.replace('Z','')}+00:00" if not datetime_str_parsed.endswith("+00:00") else datetime_str_parsed.replace('Z','')
        return datetime.fromisoformat(datetime_str_parsed), datetime_str

def sort_logs(logs):
  """Sorts a list of logs by date and time, oldest to newest.

  Args:
    logs: A list of log strings in the format "SystemTime=\"YYYY-MM-DDTHH:MM:SS.fffffffZ\"".

  Returns:
    A new list of logs sorted by date and time.
  """
  def extract_datetime(log):
    match = re.search(EVTX_PATTERNS.get("SystemTime"), log)
    if match:
        
        datetime_str = match.group(1)
        datetime_str = re.sub(r'(\.\d{6})\d+', r'\1', datetime_str)
        datetime_str = f"{datetime_str.replace('Z','')}+00:00" if not datetime_str.endswith("+00:00") else datetime_str.replace('Z','')
        return datetime.fromisoformat(datetime_str) 

  return sorted(logs, key=extract_datetime)


def update_log_times(logs, logger):
  """
  Updates the timestamps in a list of logs, closing gaps larger than 10 minutes
  while maintaining the timeline and preventing timedelta overflow.

  Args:
    logs: A list of log strings, each containing a "TimeCreated SystemTime" entry.

  Returns:
    A new list of log strings with updated timestamps.
  """
 
  
  logs = sort_logs(logs)  # Assuming you have a sort_logs function
  
  updated_logs = []
  if logs:
    
    current_timestamp, first_timestamp_str = extract_datetime(logs[0], "SystemTime")
    previous_delta = False  # Initialize cumulative time shift

    updated_logs.append(logs[0])  # Add the first log without modification

    for log in logs[1:]:
        
        timestamp, timestamp_str = extract_datetime(log, "SystemTime")


        # Calculate the time difference considering the cumulative shift
        time_delta = timestamp - current_timestamp
        gap_minutes = time_delta.total_seconds() / 60

        if gap_minutes > 5:
            if previous_delta:
                aggregated_delta = time_delta - previous_delta

                if aggregated_delta < timedelta(minutes=5):

                    

                    previous_delta = previous_delta + aggregated_delta
                    new_timestamp = current_timestamp + aggregated_delta


                elif aggregated_delta > timedelta(minutes=5):
           
                    
                    # Adjust the timestamp to be 10 minutes after the previous one
                    new_timestamp = current_timestamp + timedelta(minutes=5)
                    previous_delta = timestamp - new_timestamp

            else:      
                
                new_timestamp = current_timestamp + timedelta(minutes=5)
                previous_delta = timestamp - new_timestamp



            # Update the SystemTime and UtcTime (if present) with the new timestamp
            new_timestamp_str = new_timestamp.isoformat().replace('+00:00', 'Z')
            updated_log = log.replace(timestamp_str, new_timestamp_str)

            if '<Data Name="UtcTime">' in log:

                sysmon_timestamp, sysmon_timestamp_str = extract_datetime(log, "UtcTime")
                new_sysmon_timestamp = sysmon_timestamp + (new_timestamp - timestamp)
                new_sysmon_timestamp_str = new_sysmon_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")
                updated_log = updated_log.replace(sysmon_timestamp_str, new_sysmon_timestamp_str)

        
            
            updated_logs.append(updated_log)
            current_timestamp = new_timestamp  # Update current_timestamp

        else:
            updated_logs.append(log)
            current_timestamp = timestamp  # Update current_timestamp

    timestamps = []
    for log in updated_logs:
        timestamp, timestamp_str = extract_datetime(log, "SystemTime")
        timestamps.append(timestamp)

    earliest_timestamp = min(timestamps) if timestamps else None
    latest_timestamp = max(timestamps) if timestamps else None

    return updated_logs, earliest_timestamp, latest_timestamp

def process_file(file_path):
    # Extract the extension
    _, ext = os.path.splitext(file_path)

    # Normalize the extension (remove leading dot and lowercase)
    ext = ext.lower().lstrip(".")

    # Check if the extension is valid
    if ext in ("evtx"):
      
      if is_file_empty(file_path):
        raise ValueError("File is empty.")
        # Convert the EVTX to XML, and loads the XML file
      extract_xml_from_evtx(file_path, file_path + '.xml')
      return read_file(file_path + '.xml')
    elif ext in ("xml"):
      
      if is_file_empty(file_path):
        raise ValueError("File is empty.")
      # Loads the XML file
      return read_file(file_path)
    else:
      
      return None

def extract_events(file_path):
  
  xml_payload = process_file(file_path)

  if xml_payload:
    return find_regex_matches(xml_payload,"<Event\s*.*?<\/Event>")
  else:
    return [], xml_payload