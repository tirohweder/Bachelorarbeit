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


    MATCH (a:Address)
    WHERE a.address='3PxjQuzXzVDVfdxeXVgQmsrCnn4LLcsEod'
    RETURN a.inDegree AS inDegree, a.outDegree AS outDegree


    //Nimmt txid und guckt welche adresse das resultat ist'
    MATCH (t:Transaction)-[r:RECEIVES]->(tr:Address)
    WHERE t.txid='1bb2076b85a947ffa16892f5e66f1b8083ee8121ba390f69953391c90d6ca317'
    RETURN tr.address AS tr_address


    //nimmt txid und gibt alle einzahlenden adressen dazu

    MATCH (t:Address)-[r:SENDS]->(tr:Transaction)
    WHERE tr.txid='28f9585aa6abdf28202d759b78e531dbfc2f8f3b83592321c3414ce33504a84d'
    RETURN t.address AS tr_address

        MATCH (a:Address)-[s:SENDS]->(tr:Transaction)
        MATCH (tr)-[b:BELONGS_TO]->(bl:Block)
        WHERE tr.txid='28f9585aa6abdf28202d759b78e531dbfc2f8f3b83592321c3414ce33504a84d'
        RETURN tr.txid AS txid, a.address AS address,bl.hash AS hash, bl.mediantime AS time

    MATCH (a:Address)-[s:SENDS]->(tr:Transaction)
    MATCH (tr)-[b:BELONGS_TO]->(b:Block)
    WHERE tr.txid='28f9585aa6abdf28202d759b78e531dbfc2f8f3b83592321c3414ce33504a84d'
    RETURN t.address AS tr_address, tr.inSum, b.mediantime AS b_time

