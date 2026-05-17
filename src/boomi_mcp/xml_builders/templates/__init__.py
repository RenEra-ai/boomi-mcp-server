"""
Boomi Component XML Templates.

This module contains XML templates as Python constants for Boomi components.
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


# Custom Library Component Wrapper Template.
#
# Type values:
#   - "general"   → JARs deploy to /userlib (runtime restart required for pickup)
#   - "scripting" → JARs deploy to /userlib/script (loaded immediately)
#   - "connector" → JARs deploy to /userlib/<connector_type> (loaded immediately);
#                   {connector_type_element} must contain <connectorType>...</connectorType>
#                   (e.g. database, disk, http, ftp). For other Types, leave it empty.
#
# {files_xml} is the concatenation of one CUSTOM_LIBRARY_FILE_ENTRY per JAR.
# Packaging requires the full Files metadata (checksum, md5, size, guid) that
# Boomi assigns when the JAR is uploaded to the Account Library — name alone
# is sufficient for create, but create_package will fail with
# "Custom library references deleted jars" until the metadata is populated.
CUSTOM_LIBRARY_COMPONENT_WRAPPER = """<?xml version="1.0" encoding="UTF-8"?>
<bns:Component xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:bns="http://api.platform.boomi.com/"
               name="{name}"
               type="customlibrary"
               folderFullPath="{folder_full_path}">
  <bns:encryptedValues/>
  <bns:description>{description}</bns:description>
  <bns:object>
    <CustomLibrary xmlns="">
      <Type>{library_type}</Type>{connector_type_element}
{files_xml}
    </CustomLibrary>
  </bns:object>
</bns:Component>"""


# Single <Files .../> entry for inclusion inside CUSTOM_LIBRARY_COMPONENT_WRAPPER.
# All six attributes are required for create_package to succeed; they must match
# the values Boomi recorded when the JAR was uploaded to the Account Library.
CUSTOM_LIBRARY_FILE_ENTRY = (
    '      <Files checksum="{checksum}"'
    ' checksumType="{checksum_type}"'
    ' guid="{guid}"'
    ' md5="{md5}"'
    ' name="{name}"'
    ' size="{size}"/>'
)


__all__ = [
    "PROCESS_COMPONENT_WRAPPER",
    "CUSTOM_LIBRARY_COMPONENT_WRAPPER",
    "CUSTOM_LIBRARY_FILE_ENTRY",
]
