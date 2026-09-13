"""#184 E1 graph extensions: Try/Catch and Exception, both rendered by the branch-point LEGACY renderer
(render_catcherrors / render_exception in wt-cbab28f), wired exactly like the legacy adapters
(_emit_catcherrors: dragpoint identifiers default/Try and error/Catch; _emit_exception: MessageFormat-escaped
message, binding none)."""
import g184 as G
from g184 import R, _ctx


class GraphX(G.Graph):
    def catcherrors(self, sid, col, row, try_to, catch_to):
        edges = [(try_to, "default", "Try"), (catch_to, "error", "Catch")]
        self._add(R.render_catcherrors(_ctx(sid, col, row, edges), retry_count=0))

    def exception(self, sid, col, row, message, title="E1 deliberate exception", binding="caught_error"):
        # binding "none" renders no <exParameters>; the platform REFUSES that on create (measured E1:
        # HTTP 400 cvc-complex-type.2.4.b "One of '{exParameters}' is expected"), so the captures bind the
        # Try/Catch message into {1}, which also proves the catch path carried the caught error.
        self._add(R.render_exception(_ctx(sid, col, row, []), title=title, stop_single_document=False,
                                     message=R._escape_message_format_text(message),
                                     binding=R.RenderExceptionBinding(kind=binding)))
