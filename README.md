#Prerequisites
 - PostgreSQL Server
 - Python 3.8

# Step by Step:

In **settings.py** the different connections to the Neo4j database and PostgreSQL database can be set. 
Furthermore, the master address can be changed here as well as 



1. Execute database_script
2. Run hitbtc_trans_data.py - choose currency
3. Run potential_dep_address_and_transactions.py only find_address_transactions

4. Run get_tran_qty.py
    -  With bitcoin_client potential_depositing_transactions_with_blockhash is used,
         for the other two potential_depositing_transactions, choose one, default API as is fastest
5. Run dep_address_and_transactions.py insert_deposit_address_transactions



6. Run info_adder.py
        in_out_degree           | for_back_up
    
        get_real_in_out_degree  | gemacht
    
        conn_with_host          |

        get_usd_value_for_eth   |
    
        origin_checker          | inserts the the amount of origin addresses into table origin
    
        address_from_tagpack    |
        origin_label            |
    
        entity_adder            |
        entity_label            |
    
        density_checker         | is used for analysis


7. Run find_matches.py

