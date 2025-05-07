#!/usr/bin/env python3
"""
openEHR → CatSalut CDR ingestor  (v12.1  2025-05-01)

• atomic global _codes (with safety re-insert of _id:"ar_code")
• full key/value shortcut pass (deep)
• replication factor, safe inserts
• detailed per-EHR logging
"""

import os, json, logging, functools, threading, certifi, traceback, time
from datetime import datetime
from typing import Any, Dict, List, Tuple, Set
from bson     import ObjectId
from pymongo  import MongoClient, ReturnDocument, errors
from pymongo.errors import BulkWriteError, DuplicateKeyError
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import freeze_support
import re

# ───────── logging ─────────
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL","INFO").upper()),
    format="[%(processName)s %(asctime)s] %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("pymongo").setLevel(logging.WARNING)

# ───────── globals shared by workers ─────────
TEMPLATE_CACHE : Dict[str,Dict[Tuple[int,...],int]] = {}
TEMPLATE_NEXT  : Dict[str,int] = {}
CACHE_LOCK = threading.Lock()

LOCATABLE = {                   # quick membership test
    "COMPOSITION","SECTION","ADMIN_ENTRY","OBSERVATION","EVALUATION",
    "INSTRUCTION","ACTION","CLUSTER","ITEM_TREE","ITEM_LIST","ITEM_SINGLE",
    "ITEM_TABLE","ELEMENT","HISTORY","EVENT","POINT_EVENT","INTERVAL_EVENT",
    "ACTIVITY","ISM_TRANSITION","INSTRUCTION_DETAILS","CARE_ENTRY",
    "PARTY_PROXY","EVENT_CONTEXT"
}

NON_ARCHETYPED_RM = {                     
    "HISTORY", "EVENT", "POINT_EVENT", "INTERVAL_EVENT",
    "ISM_TRANSITION",
}

LOC_HINT = {"archetype_node_id", "archetype_details"}
SKIP_ATTRS = {"archetype_details", "uid"}

SHORTCUT_KEYS : Dict[str,str] = {}
SHORTCUT_VALS : Dict[str,str] = {}
alloc_code = None

CODE_BOOK   : dict[str, dict[str, int]] = {}   # e.g. CODE_BOOK["ar_code"][sid] = n
SEQ         : dict[str, int]            = {"ar_code": 0}

# ═════════ config ═════════
def cfg(path="config.json"):
    with open(path,encoding="utf-8") as f:
        return json.load(f)

# ═════════ _codes allocator ═════════
def make_allocator(_: str):            # coll_name is no longer needed
    """
    In‑memory allocator.
    - one global CODE_BOOK  :  {key -> {sid -> code}}
    - one global SEQ        :  {key -> last_int}
    Guaranteed to stay consistent inside a single Python process.
    """
    def alloc(key: str, sid: str) -> int:
        # if it’s a parseable at-code, just turn it into a number
        if key == "ar_code" and sid.lower().startswith("at"):
            n = at_code_to_int(sid)
            if n is not None:
                return n

        book = CODE_BOOK.setdefault(key, {})   # get / create sub‑dict
        if sid in book:                        # already allocated
            return book[sid]

        SEQ.setdefault(key, 0)                 # ensure counter exists
        SEQ[key] += 1
        code = SEQ[key]
        book[sid] = code
        return code

    return alloc

def bootstrap_codes(db, coll):
    col = db[coll]
    col.replace_one({"_id":"ar_code"},{"_id":"ar_code"},upsert=True)
    col.replace_one({"_id":"sequence"},{"_id":"sequence",
                      "seq":{"ar_code":0}},upsert=True)

# ═════════ shortcut expansion ═══════════
def apply_sc(o):
    if isinstance(o,dict):
        return {SHORTCUT_KEYS.get(k,k):apply_sc(v) for k,v in o.items()}
    if isinstance(o,list):
        return [apply_sc(x) for x in o]
    if isinstance(o,str):
        if o in SHORTCUT_VALS:
            return SHORTCUT_VALS[o]
    return o

@functools.lru_cache(maxsize=None)
def abbr(rt: str) -> str | None:
    """
    Return the shortcut only if the RM type is explicitly
    listed in shortcuts.values; otherwise None.
    """
    return SHORTCUT_VALS.get(rt)             

# ───────── claim & lock a patient (NEW) ─────────
def claim_patient_id(coll) -> str | None:
    """
    Atomically pick ONE still‑unused document, mark *all* documents of the
    same patient as used and return that EHR id.  A None return means the
    collection is exhausted.
    """
    doc = coll.find_one_and_update(
        {"used": {"$ne": True}},          # any not‑yet‑processed doc
        {"$set": {"used": True}},         # lock that single doc
        sort=[("_id", 1)],                # cheap natural order
        projection={"ehr_id": 1},
    )
    if not doc:
        return None                       # nothing left to process

    eid = doc["ehr_id"]
    # lock the *rest* of that patient so no other worker can grab it later
    coll.update_many({"ehr_id": eid, "used": {"$ne": True}},
                     {"$set": {"used": True}})
    return eid

def claim_docs_for_patient(src_coll) -> str|None:
    """
    Atomically pick **one** unprocessed document, mark all docs
    of that EHR as taken, and return the ehr_id.  Returns None when
    the collection is exhausted.
    """
    # 1️⃣ pick a single free document
    picked = src_coll.find_one_and_update(
        {"$or":[{"used":False},{"used":{"$exists":False}}]},
        {"$set":{"used":"⏳"}},                      # ⏳ = being processed
        projection={"ehr_id":1})
    if not picked:          # nothing left
        return None
    eid = picked["ehr_id"]

    # 2️⃣ mark *all* docs of that patient as “⏳”
    src_coll.update_many({"ehr_id":eid,
                          "$or":[{"used":False},{"used":{"$exists":False}}]},
                         {"$set":{"used":"⏳"}})
    return eid


def next_patient_id(src_coll, *, stop_after:int):
    """Generator that hands out up to `stop_after` patient IDs."""
    issued = 0
    while issued < stop_after:
        eid = claim_docs_for_patient(src_coll)
        if eid is None:
            break
        issued += 1
        yield eid


def get_ehr_ids(col, *, limit:int, batch:int=10_000, log_every:int=5_000):
    """
    Stream `limit` distinct EHR‑IDs from `col` without large RAM usage.
    Yields the IDs as soon as they are discovered.
    """
    qry = {"$or":[{"used":False},{"used":{"$exists":False}}]}
    seen  : set[str] = set()
    added = scanned = 0
    cursor = (col.find(qry, {"ehr_id":1})        # ← make sure this index exists
                 .batch_size(batch))

    for doc in cursor:
        scanned += 1
        eid = doc["ehr_id"]
        if eid not in seen:
            seen.add(eid)
            yield eid
            added += 1
            if added >= limit:
                break
        if scanned % log_every == 0:
            logging.info("cursor: scanned %d docs  ➜  %d new / %d total EHRs",
                         scanned, added, len(seen))

# ═════════ at‑code → int ════════════════════════════════════════

# major=any digits, optional .fraction=any digits
_AT_RE = re.compile(r"""^
    at0*([0-9]+)        # group(1) = major
    (?:\.([0-9]+))?     # optional .fraction in group(2)
$""", re.IGNORECASE | re.VERBOSE)

def at_code_to_int(at: str) -> int | None:
    at_norm = at.strip().lower()
    m = _AT_RE.match(at_norm)
    if not m:
        if at_norm.startswith("at"):
            logging.warning("at_code_to_int failed to parse archetype_node_id %r.", at)
        return None     # truly un-parsable → fallback
    major = int(m.group(1), 10)
    frac  = (m.group(2) or "").ljust(6, "0")[:6]
    return major * 1_000_000 + int(frac, 10)

def archetype_id(obj: dict) -> str | None:
    # 1️⃣ look in archetype_details.archetype_id.value
    ad = obj.get("archetype_details") or {}
    ai = ad.get("archetype_id")
    if isinstance(ai, dict) and ai.get("value"):
        return ai["value"].strip()
    if isinstance(ai, str) and ai.strip():
        return ai.strip()

    # 2️⃣ fallback to the direct archetype_node_id property if present
    ani = obj.get("archetype_node_id")
    if isinstance(ani, str) and ani.strip():
        return ani.strip()

    return None


def is_locatable(obj: dict) -> bool:
    if not isinstance(obj, dict):
        return False
    if obj.get("_type") in LOCATABLE:
        return True
    return any(k in obj for k in LOC_HINT)

# ────────────────────────────────────────────────────────────────────
#  Flatten a composition
# ────────────────────────────────────────────────────────────────────
def walk(node: dict,
         ancestors: Tuple[int, ...],
         cn: List[dict],
         *,
         inside_list: bool,
         attr_key: str | None = None) -> None:
    """
    • A new `cn` entry is created **only** when the node lives inside a list
      (i.e. it was an array element) _or_ it is the root COMPOSITION.
    • `ap` stores *ancestors only*; there is no `pp`.
    • While copying scalars into the parent pocket, *any* attribute is
      skipped when it
         – is `archetype_details`, or
         – is a **dict** that is a Locatable, or
         – is a **list** that contains (at least one) Locatable.
    """
    # ── decide whether this node gets its own record ───────────────
    aid   = archetype_id(node)
    code  = None
    if aid and aid.startswith("at"):
        code = at_code_to_int(aid)
        # **don’t** ever do `alloc_code("ar_code", aid)` if this was an “at…” string
    elif aid:
        # everything else (the proper URIs, openEHR-EHR-CLUSTER… etc)
        code = alloc_code("ar_code", aid)

    is_root  = not ancestors
    emit     = (inside_list or is_root) and code is not None

    # ── pocket the scalar attributes (only if we are *emitting*) ───
    if emit:
        scalars: Dict[str, Any] = {}
        for k, v in node.items():
            if k in SKIP_ATTRS:
                continue
            if isinstance(v, dict) and is_locatable(v):
                continue                              # drop entire sub‑object
            if isinstance(v, list) and any(is_locatable(x) for x in v):
                continue                              # drop locatable array

            scalars[k] = v

        d = apply_sc(scalars)
        d["ani"] = code
        if attr_key:
            d["ak"] = SHORTCUT_KEYS.get(attr_key, attr_key)
        rt = node.get("_type")
        if rt and rt in SHORTCUT_VALS:
            d["T"] = SHORTCUT_VALS[rt]

        cn.append({"ap": list(ancestors), "d": d})

    # ── recurse ────────────────────────────────────────────────────
    new_anc = ancestors + ((code,) if emit else ())

    # ① first: dive into **dict** attributes that are themselves Locatables
    for k, v in node.items():
        if isinstance(v, dict) and is_locatable(v):
            walk(v, new_anc, cn,
                 inside_list=False, attr_key=k)

    # ② second: dive into **list** attributes – each locatable element
    #            becomes `inside_list=True`
    for k, v in node.items():
        if isinstance(v, list):
            for it in v:
                if is_locatable(it):
                    walk(it, new_anc, cn,
                         inside_list=True, attr_key=k)

# ──────────────────────────────────────────────────────────────────
#  transform() – thin wrapper around walk()
# ──────────────────────────────────────────────────────────────────
def transform(raw: dict) -> tuple[dict, str]:
    comp      = raw["comp"]
    root_aid  = archetype_id(comp) or "unknown"
    template  = alloc_code("ar_code", root_aid)

    cn: list[dict] = []
    walk(comp, (), cn, inside_list=False)        #  ← NO  is_root=True

    return {
        "_id":         raw["_id"],
        "ehr_id":      raw["ehr_id"],
        "comp_id":      raw["comp_id"],
        "version":      raw["version"],
        "template_id": template,
        "cn":          cn,
    }, root_aid

# ═════════ safe insert ═══════════════════
def safe_insert(col,docs):
    if not docs: return 0
    try: return len(col.insert_many(docs,ordered=False).inserted_ids)
    except BulkWriteError as bwe:
        dup=sum(1 for e in bwe.details.get("writeErrors",[]) if e["code"]==11000)
        other=[e for e in bwe.details.get("writeErrors",[]) if e["code"]!=11000]
        if other: raise
        return len(docs)-dup

# ═════════ batch flag reset ═══════════════
def reset_used(coll,batch):
    while True:
        ids=[d["_id"] for d in coll.find({"used":True},{"_id":1}).limit(batch)]
        if not ids: break
        coll.update_many({"_id":{"$in":ids}},{"$unset":{"used":""}})

# ═════════ worker init ════════════════════
def init_worker(cfg: dict, ca: str) -> None:
    """
    Connects to Mongo, primes the shortcut tables and installs the
    in‑memory *alloc_code* function.  
    """
    global SRC, TGT, alloc_code

    src_cli = MongoClient(cfg["source"]["connection_string"], tlsCAFile=ca)
    tgt_cli = MongoClient(cfg["target"]["connection_string"], tlsCAFile=ca)

    SRC = src_cli[cfg["source"]["database_name"]][
        cfg["source"]["collection_name"]
    ]
    TGT = tgt_cli[cfg["target"]["database_name"]][
        cfg["target"]["compositions_collection"]
    ]

    alloc_code = make_allocator("")      # now purely in‑memory

    # ── populate the shortcut tables (keys / values) ───────────────────
    sc = tgt_cli[cfg["target"]["database_name"]][
        cfg["target"]["shortcuts_collection"]
    ].find_one({"_id": "shortcuts"}) or {}

    SHORTCUT_KEYS.update(sc.get("keys",   {}))
    SHORTCUT_VALS.update(sc.get("values", {}))

# ═════════ per-patient job ════════════════
def proc(eid, cfg):
    try:
        # we already locked all docs for this patient => no 'used' filter
        raws = list(SRC.find({"ehr_id": eid}, {"_id": 1, "ehr_id": 1, "comp": 1, "comp_id": 1, "version": 1}))
        if not raws:
            return eid, 0, 0, []

        rf = cfg.get("replication_factor", 1)
        docs, tpl_ids = [], set()

        for r in raws:
            base, tuid = transform(r); tpl_ids.add(tuid)
            for i in range(rf):
                d = base.copy()
                d["_id"] = ObjectId()
                d["ehr_id"] = f"{eid}~r{i + 1}"
                docs.append(d)

        ins = safe_insert(TGT, docs)

        return eid, len(raws), ins, []
    except Exception:
        return eid, 0, 0, [traceback.format_exc()]

# ────────────────────────────────────────────────────────────────────────
#  Flush the in‑memory dictionaries into MongoDB
# ────────────────────────────────────────────────────────────────────────
def flush_globals_into_db(tgt: MongoClient, cfg: dict) -> None:
    """
    Persists CODE_BOOK / SEQ into <codes_collection>.
    The template‑path catalogue was deleted, so only codes remain.
    """
    codes_col = tgt[cfg["target"]["database_name"]][
        cfg["target"]["codes_collection"]
    ]

    # ── build hierarchical ar_code doc ─────────────────────────────────
    book = CODE_BOOK.get("ar_code", {})
    nested: dict[str, dict[str, dict[str, int]]] = {}
    max_code = 0

    for sid, code in book.items():
        max_code = max(max_code, code)
        if sid == "unknown":
            nested["unknown"] = code
            continue
        try:
            rm, name, ver = sid.split(".", 2)
        except ValueError:
            nested[sid] = code
            continue
        nested.setdefault(rm, {}).setdefault(name, {})[ver] = code

    nested["_max"] = max_code

    codes_col.replace_one({"_id": "ar_code"},
                          {"_id": "ar_code", **nested},
                          upsert=True)

    # ── helper docs (unchanged) ─────────────────────────────────────────
    codes_col.replace_one({"_id": "sequence"},
                          {"_id": "sequence", "seq": SEQ},
                          upsert=True)
            
# ═════════ main ═══════════════════════════
def main():
    c  = cfg()
    ca = certifi.where()

    # ───── optional clean / reset steps (unchanged) ─────
    if c.get("clean_collections", False):
        tgt = MongoClient(c["target"]["connection_string"], tlsCAFile=ca)
        db  = tgt[c["target"]["database_name"]]
        logging.info("🧹 dropping target collections")
        for n in ("compositions_collection",
                  "codes_collection"):
            db[c["target"][n]].drop()
        tgt.close()

    if c.get("reset_used_flags", False):
        src = MongoClient(c["source"]["connection_string"], tlsCAFile=ca)
        logging.info("🧹 resetting source.used flags")
        reset_used(
            src[c["source"]["database_name"]][c["source"]["collection_name"]],
            c.get("batch_size_reset", 2500),
        )
        src.close()

    # ───── bootstrap the three seed documents once ─────
    tgt = MongoClient(c["target"]["connection_string"], tlsCAFile=ca)
    bootstrap_codes(
        tgt[c["target"]["database_name"]], c["target"]["codes_collection"]
    )
    tgt.close()

    # ───── ‹S E Q U E N T I A L› ingestion – no workers ─────
    init_worker(c, ca)                       # we still reuse the worker‑init

    src_coll = SRC                           # set by init_worker
    plimit   = c.get("patient_limit", 500)

    logging.info("🏃 single‑worker run, up to %d patients", plimit)

    done = fail = total_read = total_ins = 0

    for count, eid in enumerate(
            get_ehr_ids(src_coll, limit=plimit), start=1):
        eid, read, ins, errs = proc(eid, c)  # <- direct call, no pool
        total_read += read
        total_ins  += ins
        if errs:
            fail += 1
            logging.error("✖ %s failed\n%s", eid, errs[0])
        else:
            done += 1

        if count % 25 == 0:
            logging.info("… processed %d / %d", count, plimit)

    logging.info("✓ done %d  ✖ failed %d   source %d  inserted %d",
                 done, fail, total_read, total_ins)

    tgt_cli = MongoClient(c["target"]["connection_string"], tlsCAFile=ca)
    flush_globals_into_db(tgt_cli, c)
    logging.info("ar_code next value will be %d", SEQ["ar_code"] + 1)
    tgt_cli.close()
    
if __name__=="__main__":
    freeze_support(); main()
