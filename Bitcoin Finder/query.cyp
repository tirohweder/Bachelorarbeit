        MATCH (t:Transaction)-[r:RECEIVES]->(tr:Address)
        WHERE tr.address='1EEqRvnS7XqMoXDcaGL7bLS3hzZi1qUZm1'
        RETURN t.txid AS t_txid


    MATCH (a:Address)-[s:SENDS]->(tr:Transaction)
    MATCH (tr)-[b:BELONGS_TO]->(bl:Block)
    WHERE tr.txid='60412a32cc198145332bc545845a6cd0738732d2e51af2082699f2476804a334'
    RETURN tr.txid AS txid, a.address AS address,bl.hash AS hash, bl.mediantime AS time

    MATCH (t:Transaction)-[r:RECEIVES]->(tr:Address)
    MATCH (t)-[b:BELONGS_TO]->(bl:Block)
    WHERE tr.address='3LDrZPVnhRkGnsYiLLL143AZJd3XpPukfU'
    RETURN t.txid AS txid, bl.hash AS hash, bl.mediantime AS time, tr.address AS address


    MATCH (t:Transaction)-[r:RECEIVES]->(tr:Address)
    MATCH (t)-[b:BELONGS_TO]->(bl:Block)
    WHERE tr.address='3PxjQuzXzVDVfdxeXVgQmsrCnn4LLcsEod'
    RETURN t.txid AS txid


    MATCH (t:Transaction)-[r:RECEIVES]->(tr:Address)
    WHERE tr.address='3PxjQuzXzVDVfdxeXVgQmsrCnn4LLcsEod'
    RETURN t.txid AS t_txid