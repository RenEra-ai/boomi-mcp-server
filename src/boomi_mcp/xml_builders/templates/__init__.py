"""
Boomi Process XML Templates.

This module contains XML templates as Python constants for Boomi process components.
Templates follow the hybrid architecture pattern: structure in templates, logic in builders.
"""

# Process Component Wrapper Template
PROCESS_COMPONENT_WRAPPER = """<?xml version="1.0" encoding="UTF-8"?>
<bns:Component xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:bns="http://api.platform.boomi.com/"
               name="{name}"
               type="process"
               folderName="{folder_name}"
               {folder_id_attr}>
  <bns:encryptedValues/>
  <bns:description>{description}</bns:description>
  <bns:object>
    <process xmlns=""
             allowSimultaneous="{allow_simultaneous}"
             enableUserLog="{enable_user_log}"
             processLogOnErrorOnly="{process_log_on_error_only}"
             purgeDataImmediately="{purge_data_immediately}"
             updateRunDates="{update_run_dates}"
             workload="{workload}">
      <shapes>
{shapes}
      </shapes>
    </process>
  </bns:object>
  <bns:processOverrides/>
</bns:Component>"""

__all__ = ["PROCESS_COMPONENT_WRAPPER"]
