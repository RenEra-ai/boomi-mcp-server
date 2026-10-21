"""Grade x29 base -> work: every move, against the flattened twin."""
import json, collections, sys
b = json.load(open('out/x29_hunt_base.json')); n = json.load(open('out/x29_hunt_%s.json' % (sys.argv[1] if len(sys.argv) > 1 else 'work')))
assert set(b) == set(n)
cls = collections.Counter(); adm = collections.Counter(); bad = []
for k in sorted(b):
    B, N = b[k], n[k]
    if 'error' in B or 'error' in N:
        cls[('ERROR', B.get('error'), N.get('error'))] += 1; continue
    cls[(B['class'], N['class'])] += 1
    ba, na, ta = not B['comp_codes'], not N['comp_codes'], not N['twin_codes']
    if ba and not na:
        adm['admission->refusal, twin ' + ('admits' if ta else 'refuses')] += 1
        if ta: bad.append(('ADM->REF twin admits', k))
    elif na and not ba:
        adm['refusal->admission, twin ' + ('admits' if ta else 'refuses')] += 1
        if not ta: bad.append(('REF->ADM twin refuses', k))
    elif B != N:
        adm['moved, same admission (%s)' % ('admitted' if na else 'refused')] += 1
    if N['class'] == 'ESCAPE' and B['class'] != 'ESCAPE' and B['class'] != 'HIT':
        bad.append(('NEW ESCAPE', k, B['class']))
    if B['twin_codes'] != N['twin_codes']:
        adm['twin codes moved: %s->%s' % ('adm' if not B['twin_codes'] else 'ref', 'adm' if ta else 'ref')] += 1
for k, v in sorted(cls.items(), key=str): print(v, k)
for k, v in sorted(adm.items()): print(v, k)
print('violations', len(bad)); [print(x) for x in bad[:30]]
