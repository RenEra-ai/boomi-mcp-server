"""#184 E0 graph composer.

Every shape is rendered by the LEGACY renderer at the branch point (the pristine
worktree's process_emitters/rendering.py), with two hand-authored exceptions,
each recorded as provenance:
  * the Data Passthrough start (`<passthroughaction/>`) and the passthrough
    process's `<process>` option attributes, hand-authored from the UI-built
    live capture tests/fixtures/live_xml/m11/process_doccacheretrieve_loadalldoc_variant.xml;
  * the Groovy fan-out script text (the dataprocess shape around it is the
    legacy renderer's custom_scripting form).
Nothing here imports or calls any #184 code (none exists at the branch point).
"""
import os, re
import xml.etree.ElementTree as ET
import lib184 as L

L.srv()
from boomi_mcp.categories.components.builders.process_emitters import rendering as R  # noqa: E402
from boomi_mcp.categories.components.builders.process_emitters import legacy as LG  # noqa: E402
from boomi_mcp.categories.components.process_component_materializer import DEFAULT_PROCESS_OPTIONS  # noqa: E402

for _m in (R, LG):
    assert os.path.realpath(_m.__file__).startswith(os.path.realpath(L.WT)), _m.__file__

UI_FIXTURE = "tests/fixtures/live_xml/m11/process_doccacheretrieve_loadalldoc_variant.xml"
UI_PASSTHROUGH_OPTIONS = ('allowSimultaneous="false" enableUserLog="false" processLogOnErrorOnly="false" '
                          'purgeDataImmediately="false" updateRunDates="false" workload="general"')

PROV_LEGACY = "legacy renderer at branch point cbab28f (wt-cbab28f process_emitters/rendering.py)"
PROV_PT = f"hand-authored from UI-built live capture {UI_FIXTURE} (shape1 <passthroughaction/> + <process> option attributes)"


def _ctx(sid, col, row, edges=()):
    x = R._shape_x(col)
    y = R._SHAPE_Y + row * 160.0
    trs = []
    for i, e in enumerate(edges, start=1):
        to, ident, text = (e if isinstance(e, tuple) else (e, None, None))
        trs.append(R.RenderTransition(dragpoint_name=f"{sid}.dragpoint{i}", to_shape_id=to,
                                      x=R._dragpoint_x(col), y=y + 8.0, identifier=ident, text=text))
    return R.ShapeRenderContext(shape_id=sid, x=x, y=y, transitions=tuple(trs))


class Graph:
    def __init__(self, name, passthrough=False):
        self.name = name
        self.passthrough = passthrough
        self.parts = []
        self.prov = set()

    def _add(self, xml, prov=PROV_LEGACY):
        self.parts.append(xml)
        self.prov.add(prov)

    # ---- starts
    def start(self, sid, col, row, nxt):
        x = R.render_start_noaction(_ctx(sid, col, row, [nxt]))
        if self.passthrough:
            assert x.count("<noaction/>") == 1
            self._add(x.replace("<noaction/>", "<passthroughaction/>"), PROV_PT)
        else:
            self._add(x)

    # ---- linear
    def message(self, sid, col, row, text, nxt):
        self._add(R.render_message(_ctx(sid, col, row, [nxt]), userlabel="", text=text))

    def groovy(self, sid, col, row, script, nxt, label=""):
        step = LG._legacy_dp_step({"operation": "custom_scripting", "script": script}, 1)
        self._add(R.render_dataprocess(_ctx(sid, col, row, [nxt]), userlabel=label, steps=(step,)))

    def setprops(self, sid, col, row, assignments, nxt, label=""):
        """assignments: [(scope 'ddp'|'dpp', name, [RenderPropertySource...])]"""
        props = ""
        for scope, name, sources in assignments:
            sv = "".join(R.render_property_source_value(i, s) for i, s in enumerate(sources, start=1))
            props += R.render_documentproperty_assignment(scope, name, False, sv)
        self._add(R.render_setproperties_shape(_ctx(sid, col, row, [nxt]), userlabel=label, properties_xml=props))

    def connector(self, sid, col, row, op_id, nxt, verb="POST", ddp=None, label=""):
        dp = R.RenderConnectorDynamicPath(ddp_name=ddp, request_profile_id="", has_profile_segment=False) if ddp else None
        self._add(R.render_connectoraction(_ctx(sid, col, row, [nxt]), userlabel=label, connector_type=L.REST_SUB,
                                           action_type=verb, connection_id=L.REST_CONN, operation_id=op_id,
                                           dynamic_path=dp))

    def cache_load(self, sid, col, row, cache_id, nxt=None):
        self._add(R.render_doccacheload(_ctx(sid, col, row, [nxt] if nxt else []), userlabel="", doc_cache_id=cache_id))

    def cache_retrieve(self, sid, col, row, cache_id, nxt):
        self._add(R.render_doccacheretrieve(_ctx(sid, col, row, [nxt]), userlabel="", doc_cache_id=cache_id,
                                            empty_cache_behavior="stopprocess"))

    def cache_remove(self, sid, col, row, cache_id, nxt):
        self._add(R.render_doccacheremove(_ctx(sid, col, row, [nxt]), userlabel="", doc_cache_id=cache_id))

    def flowcontrol(self, sid, col, row, nxt, n=1):
        self._add(R.render_flowcontrol(_ctx(sid, col, row, [nxt]), userlabel="", for_each_count=n))

    def map(self, sid, col, row, map_id, nxt):
        self._add(R.render_map(_ctx(sid, col, row, [nxt]), userlabel="", map_id=map_id))

    # ---- control
    def branch(self, sid, col, row, legs):
        edges = [(to, str(i), str(i)) for i, to in enumerate(legs, start=1)]
        self._add(R.render_branch(_ctx(sid, col, row, edges), userlabel="", num_branches=len(legs)))

    def decision_true(self, sid, col, row, true_to, false_to):
        edges = [(true_to, "true", "True"), (false_to, "false", "False")]
        v = R.RenderDecisionValue(value_type="static", static_value="go")
        self._add(R.render_decision(_ctx(sid, col, row, edges), label="static go equals go", comparison="equals",
                                    left=v, right=v))

    # ---- terminals
    def processcall(self, sid, col, row, pid, wait=True, abort=True):
        self._add(R.render_processcall(_ctx(sid, col, row, []), userlabel="", process_id=pid, wait=wait, abort=abort))

    def stop(self, sid, col, row):
        self._add(R.render_stop(_ctx(sid, col, row, []), continue_=True))

    # ---- envelope (byte form of the materializer's legacy envelope)
    def xml(self):
        opts = UI_PASSTHROUGH_OPTIONS if self.passthrough else DEFAULT_PROCESS_OPTIONS
        body = "".join(self.parts)
        x = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
             '<bns:Component xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
             'xmlns:bns="http://api.platform.boomi.com/" '
             f'type="process" name="{self.name}">'
             '<bns:encryptedValues/><bns:description></bns:description><bns:object>'
             f'<process xmlns="" {opts}><shapes>{body}</shapes></process>'
             '</bns:object><bns:processOverrides/></bns:Component>')
        root = ET.fromstring(x)
        names = [s.get("name") for s in root.iter("shape")]
        assert len(names) == len(set(names)), names
        for dp in root.iter("dragpoint"):
            assert dp.get("toShape") in names, (self.name, dp.get("toShape"))
        starts = [s for s in root.iter("shape") if s.get("shapetype") == "start"]
        assert len(starts) == 1
        return x


def shapes_of(xml):
    m = re.search(r"<shapes>.*</shapes>", xml, re.S)
    return m.group(0) if m else ""


def process_options_of(xml):
    m = re.search(r"<process xmlns=\"\"([^>]*)>", xml)
    return m.group(1).strip() if m else None


def src_static(v):
    return R.RenderPropertySource(value_type="static", value=v)


def src_profile(profile_id, element_key, element_name):
    return R.RenderPropertySource(value_type="profile", element_id=str(element_key), element_name=element_name,
                                  profile_id=profile_id, profile_type="profile.json")


def src_dpp(name):
    return R.RenderPropertySource(value_type="dpp", property_name=name)


def fanout_script(prefix, n=3):
    return ("import java.util.Properties\n"
            "import java.io.ByteArrayInputStream\n"
            f"for (int i = 1; i <= {n}; i++) {{\n"
            f"    String body = '{{\"k\":\"{prefix}' + i + '\"}}'\n"
            "    dataContext.storeStream(new ByteArrayInputStream(body.getBytes(\"UTF-8\")), new Properties())\n"
            "}\n")


PASSTHROUGH_SCRIPT = ("import java.util.Properties;\nimport java.io.InputStream;\n\n"
                      "for( int i = 0; i < dataContext.getDataCount(); i++ ) {\n"
                      "    InputStream is = dataContext.getStream(i);\n"
                      "    Properties props = dataContext.getProperties(i);\n\n"
                      "    dataContext.storeStream(is, props);\n}")
