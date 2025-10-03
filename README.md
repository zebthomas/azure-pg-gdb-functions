Functions for creating, managing, and migrating to Azure Postgres Flex Geodatabases

Utility Functions
find_db_connections: Find db connection files for a database
register_with_datastore: Register an Azure database with ArcGIS Server datastores
add_ro_user: Add a read-only user for a database schema
terminate_user_connections: Terminate all connections from a user to a database. Useful when refreshing db data.

Main Functions
create_and_enable_azure_geodatabase: Create a geodatabase in Azure Postgres Flex Server
copy_db_to_azure: Copy a database from on-prem to Azure
migrate_database: From a given on-prem connection, derive the inputs for
    create_and_enable_azure_geodatabase and copy_db_to_azure and run them
