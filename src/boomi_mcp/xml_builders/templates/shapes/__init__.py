"""
Boomi Process Shape Templates.

Individual shape templates extracted from real Boomi processes.
These templates represent the structure while builders handle the logic.
"""

# Start Shape Template
START_SHAPE_TEMPLATE = """        <shape image="start"
               name="{name}"
               shapetype="start"
               userlabel="{userlabel}"
               x="{x}"
               y="{y}">
          <configuration>
            <noaction/>
          </configuration>
          <dragpoints>
{dragpoints}
          </dragpoints>
        </shape>"""

# Stop/Return Documents Shape Template
RETURN_DOCUMENTS_SHAPE_TEMPLATE = """        <shape image="returndocuments_icon"
               name="{name}"
               shapetype="returndocuments"
               userlabel="{userlabel}"
               x="{x}"
               y="{y}">
          <configuration>
            <returndocuments label="{label}"/>
          </configuration>
          <dragpoints/>
        </shape>"""

# Map Shape Template
MAP_SHAPE_TEMPLATE = """        <shape image="map_icon"
               name="{name}"
               shapetype="map"
               userlabel="{userlabel}"
               x="{x}"
               y="{y}">
          <configuration>
            <map mapId="{map_id}"/>
          </configuration>
          <dragpoints>
{dragpoints}
          </dragpoints>
        </shape>"""

# Note Shape Template (for documentation)
NOTE_SHAPE_TEMPLATE = """        <shape image="note_icon"
               name="{name}"
               shapetype="note"
               x="{x}"
               y="{y}">
          <configuration>
            <note createdBy="{created_by}">
              <noteText>{note_text}</noteText>
            </note>
          </configuration>
          <dragpoints/>
        </shape>"""

# Stop Shape Template (process termination)
STOP_SHAPE_TEMPLATE = """        <shape image="stop_icon"
               name="{name}"
               shapetype="stop"
               userlabel="{userlabel}"
               x="{x}"
               y="{y}">
          <configuration>
            <stop continue="{continue}"/>
          </configuration>
          <dragpoints/>
        </shape>"""

# Message Shape Template (for logging/debugging)
MESSAGE_SHAPE_TEMPLATE = """        <shape image="message_icon"
               name="{name}"
               shapetype="message"
               userlabel="{userlabel}"
               x="{x}"
               y="{y}">
          <configuration>
            <message combined="false">
              <msgTxt>{message_text}</msgTxt>
              <msgParameters/>
            </message>
          </configuration>
          <dragpoints>
{dragpoints}
          </dragpoints>
        </shape>"""

# Connector Shape Template (for external system integration)
CONNECTOR_SHAPE_TEMPLATE = """        <shape image="connector_icon"
               name="{name}"
               shapetype="connector"
               userlabel="{userlabel}"
               x="{x}"
               y="{y}">
          <configuration>
            <connector>
              <connectorId>{connector_id}</connectorId>
              <operation>{operation}</operation>
              <objectType>{object_type}</objectType>
            </connector>
          </configuration>
          <dragpoints>
{dragpoints}
          </dragpoints>
        </shape>"""

# Decision Shape Template (for conditional branching)
DECISION_SHAPE_TEMPLATE = """        <shape image="decision_icon"
               name="{name}"
               shapetype="decision"
               userlabel="{userlabel}"
               x="{x}"
               y="{y}">
          <configuration>
            <decision>
              <decisionvalue>{expression}</decisionvalue>
            </decision>
          </configuration>
          <dragpoints>
{dragpoints}
          </dragpoints>
        </shape>"""

# Dragpoint Template (reusable)
DRAGPOINT_TEMPLATE = """            <dragpoint name="{name}"
                       toShape="{to_shape}"
                       x="{x}"
                       y="{y}"/>"""

__all__ = [
    "START_SHAPE_TEMPLATE",
    "RETURN_DOCUMENTS_SHAPE_TEMPLATE",
    "STOP_SHAPE_TEMPLATE",
    "MAP_SHAPE_TEMPLATE",
    "MESSAGE_SHAPE_TEMPLATE",
    "CONNECTOR_SHAPE_TEMPLATE",
    "DECISION_SHAPE_TEMPLATE",
    "NOTE_SHAPE_TEMPLATE",
    "DRAGPOINT_TEMPLATE",
]
