#Prerequisites
 - PostgreSQL 12.11 
 - Python 3.9.0
 - Neo4j 4.4.6 community

# Step by Step:

In **settings.py** the different connections to the Neo4j database and PostgreSQL database can be set. 
Furthermore, the master address can be changed here as well as 



1. Execute database_script
2. Run **hitbtc_trans_data.py** - choose currency in settings
3. After transaction data for ETH was collected run **info_adder.py** set_usd_value_for_eth    
4. Run **dep_address_and_transactions.py** 
5. Run **get_tran_qty.py**
6. Run **find_matches.py**


In order of finding the origin of addresses following methods can be used
      
        origin_checker          | inserts the the amount of origin addresses into table origin
    
        address_from_tagpack    | insert addresses from tagpacks into table tags
        origin_label            | Checks top 100 addresses by appearance against GraphSense
    
        entity_adder            | Adds entity for top 100 addresses by appearance against GraphSense
        entity_label            | Checks entitiy label for top 100 addresses by appearance
    
        


density_checker() in **info_adder.py** is used for Table 5.7.

**collection.py** is used to analyse data and create table and graphs, explained in comments