# Azure DB Functions
# Zeb Thomas
# Created: 3/19/2025

"""Functions for creating, managing, and migrating to Azure Postgres Flex Geodatabases

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

"""

import os
import glob
import sys
import arcpy
import connection_info as CI
# connection_info must include the following variables
# - host_names: a list of host names that come in front of the instance_suffix
# - instance_suffix
# - sde_pw (sde user's password)
# - pg_user (postgres admin username)
# - pg_pw
# - on_prem_pg_user (our on-prem username was different than in Azure)
# - on_prem_pg_pw
# - ags_admin_connections: a list of URLs or .ags files with admin access for registering with the datastore, e.g.
#       URLs for federated servers, .ags files for non-federated servers
#       e.g. ['https://xxx.state.mn.us/server1/admin','server2 on xxx.state.mn.us.ags')]
# Also include the following function definition which returns a password
# def getpassword(username: str, ro: bool=False) -> str:

try:
    import psycopg2
except ModuleNotFoundError as e:
    print('Warning: failed to import psycopg2:', e)
    print('only arcpy portions will work')

# constant inputs
host_names = CI.host_names
instance_suffix = CI.instance_suffix
prod_instance_prefix = CI.prod_instance_prefix
test_instance_prefix = CI.test_instance_prefix
sde_pw = CI.sde_pw
pg_user = CI.pg_user
pg_pw = CI.pg_pw
on_prem_pg_user = CI.on_prem_pg_user
on_prem_pg_pw = CI.on_prem_pg_pw
script_dir = os.path.dirname(sys.argv[0])
connections_dir = script_dir
authorization_file = os.path.join(connections_dir, "authorization_CEA8.ecp") #Z
sde_connections_dir = os.path.join(connections_dir, "sde admin user connections")
azure_connections_dir = os.path.join(connections_dir, "azure")


# Utility Functions
# -----------------

# These first few functions extract information from sde connection files
# by relying on DNR's naming convention and file organization:
# host-pg-db-user.sde or host-pg-db.sde (if db and user are the same)

def find_db_connections(db_name):
    """convenience function to find the full connection name matches for any database name"""
    search_strs = [f'*-{db_name}.sde',f'*-{db_name}-*.sde']
    dirs = [connections_dir, os.path.join(connections_dir, '*')]
    cons = []
    for d in dirs:
        for s in search_strs:
            cons += glob.glob(os.path.join(d, s))
    for c in cons:
        print(c)
    return cons

def connection_is_azure(sde_file):
    """Detect whether the connection file name indicates that it is an Azure connection"""
    con = os.path.basename(sde_file).lower().replace('.sde','')
    return 'azure' in db_connection_path or con.startswith(prod_instance_prefix) or con.startswith(test_instance_prefix)

def get_connection_info(sde_file):
    """Extract connection info (host, database, username) from the connection file name
    Argument is any connection file named with the standard naming convention
    host-pg-db-user or host-pg-db (if db and user are the same)"""

    con = os.path.basename(sde_file).lower().replace('.sde','').replace('-dc-','-') # some older connections included -dc- for direct connect

    split_con = os.path.basename(con).split('-')
    if connection_is_azure(sde_file):
        host = '-'.join(split_con[:2]) # e.g. r29p-hostgen
    else:
        host = split_con[0]
    database = split_con[-2]
    username = split_con[-1]
    if len(split_con) == 3:
        database = username
    return host, database, username

def get_ro_username(sde_file):
    """check if there is a read-only connection to a database and return username if applicable"""
    ro_username = None
    ro_connections = glob.glob(sde_file.replace('.sde','*ro.sde'))
    if ro_connections:
        ro_username = get_connection_info(ro_connections[0])[2]
    return ro_username

def get_connection_div(sde_file):
    """Determine the destination Azure instance for a database to be migrated.
    On-prem connection files are arranged in divisional folders, but for divisions
    without a dedicated instance, the default is the 'gen' instance."""

    if os.path.dirname(sde_file) == connections_dir or os.path.dirname(os.path.dirname(sde_file)).rstrip(os.sep) != connections_dir:
        return None
    div = os.path.basename(os.path.dirname(sde_file))
    host_name_suffixes = [s[-3:] for s in host_names]
    if div not in host_name_suffixes:
        div = 'gen'
    return div

# These next few functions can be used standalone or as part of the main functions for specific db management tasks

def register_with_datastore(con_or_dir, on_prem=False):
    """Attempts to register the connection(s) with all servers listed in CI.ags_admin_connections.
    These can either be admin URLs (for Enterprise-federated servers) or .ags files for non-federated, e.g.
    ['https://xxx.state.mn.us/server1/admin','server2 on xxx.state.mn.us.ags')]"""
    # Check if the folder or connection exist(s)
    if os.path.isdir(con_or_dir):
        sde_files = [f for f in glob.glob(os.path.join(sde_folder, '*.sde'))]
    elif not con_or_dir.endswith('.sde'):
        print("The provided file is not an sde connection file")
        return
    elif os.path.exists(con_or_dir):
        sde_files = [con_or_dir]
    else:
        con = os.path.join(azure_connections_dir, con_or_dir)
        if os.path.exists(con):
            sde_files = [con]
        else:
            print("The provided folder path or connection file does not exist.")
            return

    if len(sde_files) > 1:
        print('Found', len(sde_files), 'connections in', sde_folder)
    else:
        print('Found', sde_files[0])

    # ags files assumed to be in the script/connections directory
    ags_admin_connections = [c if c.startswith('http') else os.path.join(connections_dir, c) for c in CI.ags_admin_connections]
    if on_prem:
        ags_admin_connections = ['MY_HOSTED_SERVICES']

    for sde_file in sde_files:
        con_name = os.path.basename(sde_file).replace('.sde','')
        print(con_name)
        for ags_con in ags_admin_connections:
            server = os.path.basename(ags_con).split()[0]
            # Access the specific server admin via the ags file
            try:
                arcpy.AddDataStoreItem(ags_con, 'DATABASE', con_name, sde_file)
                print(f"\tRegistered {con_name} to {server} datastore successfully.")
            except Exception as e:
                print(f"\tFailed to register {con_name}: {e}")

def add_ro_user(db_connection_path, ro_username=None, ro_password=None, grant_default=True, grant_all_existing=False):
    """Add a read-only user to an existing Azure postgres SDE database and create its connection file
    The first part of the script accomplishes the equivalent of the following command, which cannot be run in Azure
    arcpy.CreateDatabaseUser_management(sde_connection_path,"DATABASE_USER",ro_username,ro_password)
    if no username is provided, username is the owner username with _ro added.
    if no password is provided, ro_password == get_password(ro_username,ro=True)
    grant_default will cause read-only access to be granted to the ro user on any newly added table to the main schema
    grant_all_existing will perform a one-time grant of read-only access on all existing tables in the main schema"""

    if not os.path.exists(db_connection_path):
        db_connection_path = os.path.join(azure_connections_dir, db_connection_path)
    if not connection_is_azure(db_connection_path):
        print('Error: Connection appears not to be for Azure')
        return

    host, database, username = get_connection_info(db_connection_path)
    instance = host + instance_suffix
    connection_description = host + '-' + database
    sde_connection = connection_description + "-sde.sde"
    sde_connection_path = os.path.join(sde_connections_dir, sde_connection)
    if not ro_username:
        ro_username = username + '_ro'
    if not ro_password:
        ro_password = CI.get_password(ro_username, ro=True)
    print(ro_username)
    ro_connection = connection_description + "-" + ro_username + ".sde"

    db_sql = f"""
        CREATE USER {ro_username} WITH PASSWORD '{ro_password}' ROLE postgresadmin;
        CREATE SCHEMA {ro_username};
        ALTER SCHEMA {ro_username} OWNER TO {ro_username};

        GRANT ALL ON SCHEMA {ro_username} TO {ro_username};
        GRANT USAGE ON SCHEMA {ro_username} TO public;"""

    con = None
    try:
        con = psycopg2.connect(database=database, user=pg_user, password=pg_pw, host=instance)
        cur = con.cursor()
        for command in db_sql.split('\n'):
            if not command: continue # skip blank spacing rows
            if command.strip().startswith('CREATE USER '):
                rolname = command.split()[2]
                cur.execute(f"SELECT 1 FROM pg_roles WHERE rolname='{rolname}'")
                if cur.fetchone() == (1,): continue # skip create user command if role already exists
            print(command)
            result = cur.execute(command)
            if result: print('\t', result)
        con.commit()
        print("Added read-only user", ro_username, "to", database)
        if grant_default: # Grant RO user default select access to newly created tables
            command = f"ALTER DEFAULT PRIVILEGES FOR ROLE {username} IN SCHEMA {username} GRANT SELECT ON TABLES TO {ro_username};"
            cur.execute(command)
            print("Granted default read access to RO user for all newly created tables")
        if grant_all_existing: # retroactively grant on an existing schema
            command = f"GRANT SELECT ON ALL TABLES IN SCHEMA {username} TO {ro_username}"
            cur.execute(command)
            print("Granted read access to RO user for all existing tables")
        con.commit()
    except psycopg2.DatabaseError as e:
        print('\nEncountered database error:')
        print(e)
        if con:
            con.rollback()
    finally:
        try: cur.close()
        except: pass
        try: con.close()
        except: pass

    arcpy.CreateDatabaseConnection_management(azure_connections_dir,ro_connection,"POSTGRESQL",instance,"DATABASE_AUTH",ro_username,ro_password,"SAVE_USERNAME",database)#,schema="#",version_type="TRANSACTIONAL",version="sde.DEFAULT",date="#")
    print(arcpy.GetMessages())
    register_with_datastore(os.path.join(azure_connections_dir,ro_connection))

# example call if you have a .sde file or path
# get_user_connections(*get_connection_info(sde_connection_path))
def get_user_connections(host, db, user):
    """Find all connections from user to db"""
    if connection_is_azure(host) and instance_suffix not in host:
        host += instance_suffix
    sql = f"""SELECT pid, usename, datname, client_addr
    FROM pg_stat_activity
    WHERE usename = '{user}';"""
    result = run_query(sql, db, host)
    print(result)
    return result

def terminate_user_connections(host, db, user):
    """Terminate all connections from user to db"""
    if connection_is_azure(host) and instance_suffix not in host:
        host += instance_suffix
    if not get_user_connections(host, db, user):
        return # only terminate if there are connections
    print('Terminating', user, 'connections . . .')
    sql = f"""SELECT pg_terminate_backend(pid)
              FROM pg_stat_activity
              WHERE usename = '{user}';"""
    result = run_query(sql, db, host) # e.g. [(True,), (True,), (True,), (True,), (True,), (True,), (True,), (True,), (True,), (True,), (True,), (True,), (True,), (True,), (True,), (True,)]
    if result:
        if not [r for r in result if r != (True,)]:
            print('\tTerminated', len(result), 'connections')
        else:
            print('\tError: unexpected result when termininating connections:', result)


# Main Functions and their helper functions
# -----------------------------------------

def create_and_enable_azure_geodatabase(host, database, username=None, password=None, ro_username=None, ro_password=None, prod=True, register_prod=True):
    """Create an Enteprise database in Azure postgres flex server
    First runs a series of SQL commands to create the database, user, schema, and permissions.
    Then runs arcpy.EnableEnterpriseGeodatabase_management
    This essentially replaces on-prem arcpy.CreateEnterpriseGeodatabase_management

    Also creates connection files, and, if applicable,
    creates read-only user and registers with the data store

    Prerequisites:
    1. Flex server already exists
    2. Postgis has already been enabled

    Works whether or not any other dbs have been created and whether or not the sde user exists yet
    By default this db is created in prod and is registered to the prod server datastores

    Example usage
    create_and_enable_azure_geodatabase('gen', 'gdrs_utm', 'gdrs', 'super_good_password', '')
    The empty quotes in the ro_username argument lead to the creation of the default read-only user.
    Omit this argument to not create a read-only user"""

    # helper function for running SQL for db creation tasks
    def runsql(commands, db='postgres'):
        con = None
        sql_succeeded = False
        sql_ran = False

        try:
            con = psycopg2.connect(database=db, user=pg_user, password=pg_pw, host=instance)
            if db == 'postgres': # if we're creating a new db, we have to do that outside of a transaction block
                con.set_isolation_level(0)
            cur = con.cursor()
            for command in commands.split('\n'):
                if not command: continue # skip blank spacing rows
                if command.strip().startswith('CREATE USER '):
                    rolname = command.split()[2]
                    cur.execute(f"SELECT 1 FROM pg_roles WHERE rolname='{rolname}'")
                    if cur.fetchone() == (1,): continue # skip create user command if role already exists
                print(command)
                result = cur.execute(command)
                if result: print('\t', result)
                sql_ran = True

            if db != 'postgres':
                con.commit()
            sql_succeeded = True
        except psycopg2.DatabaseError as e:
            print('\nEncountered database connection error:')
            print(e)
            if con:
                con.rollback()
        finally:
            try:
                cur.close()
            except:
                pass
            try:
                con.close()
            except:
                pass

        if not sql_succeeded: sys.exit(1)
        return sql_succeeded and sql_ran

    if prod: # prod is the default
        instance_prefix = prod_instance_prefix
    else:
        instance_prefix = test_instance_prefix

    if not host: # default to gen, the first entry
        host = host_names[0]
    elif host not in host_names: # allow option to choose full server name or just the 3-character identifier
        host_name_suffixes = [s[-3:] for s in host_names]
        if host in host_name_suffixes:
            try:
                i = host_name_suffixes.index(host)
            except ValueError:
                print('Error, invalid host name. Choose from:', ', '.join(host_names))
                return
            host = host_names[i]

    instance = instance_prefix + host + instance_suffix
    print("Database will be created on ", instance)
    if not username:
        username = database
    print("Username =", username)
    if not password:
        password = username
        if '_' in username:
            password = ''.join([w[0] for w in username.split('_')])
        password += 'adm1n'
    print("Password =", password)

    connection_description = instance_prefix + host + '-' + database
    sde_connection = connection_description + "-sde.sde"
    sde_connection_path = os.path.join(sde_connections_dir, sde_connection)
    # include the username in the connection file name if it differs from the db name
    db_connection = connection_description + ("" if username == database else "-" + username) + ".sde"
    db_connection_path = os.path.join(azure_connections_dir, db_connection)

    print("Running SQL Commands:")
    print("---------------------")

    # Add ROLE postgresadmin to all user creations so postgresadmin has rights to use that user
    # Add ROLE sde to all user creations so sde has rights to terminate connections for that user
    create_user = f"CREATE USER sde with PASSWORD '{sde_pw}' IN ROLE azure_pg_admin ROLE {pg_user};"
    create_db = f"CREATE DATABASE {database} WITH OWNER = sde;"
    if runsql(create_user):
        print("Created sde user in", instance)
    runsql(create_db)
    print("Created database", database)

    db_sql = f"""
    CREATE EXTENSION postgis SCHEMA public;

    CREATE SCHEMA sde;
    ALTER SCHEMA sde OWNER TO sde;

    GRANT ALL ON SCHEMA sde TO sde;
    GRANT USAGE ON SCHEMA sde TO public;

    ALTER DATABASE "{database}" SET search_path = "$user", public, sde;
    GRANT ALL ON DATABASE "{database}" TO public;
    GRANT ALL ON DATABASE "{database}" TO sde;

    CREATE USER {username} WITH PASSWORD '{password}' ROLE postgresadmin, sde;
    CREATE SCHEMA {username};
    ALTER SCHEMA {username} OWNER TO {username};

    GRANT ALL ON SCHEMA {username} TO {username};
    GRANT USAGE ON SCHEMA {username} TO public;"""

    runsql(db_sql, db=database)
    print("Created required schemas and users and granted required privileges\n")

    if not os.path.exists(sde_connection_path):
        print("Creating SDE connection:")
        print("------------------------")
        arcpy.CreateDatabaseConnection_management(sde_connections_dir,sde_connection,"POSTGRESQL",instance,"DATABASE_AUTH","sde",sde_pw,"SAVE_USERNAME",database)#,schema="#",version_type="TRANSACTIONAL")
        print(arcpy.GetMessages())
        print("")

    try:
        print("Enabling Geodatabase:")
        print("---------------------")
        arcpy.EnableEnterpriseGeodatabase_management(sde_connection_path, authorization_file)
    except Exception as e:
        print("ERROR: Failed to Enable Enterprise Geodatabase")
        print(e)
        print(arcpy.GetMessages())
        sys.exit(1)
    print(arcpy.GetMessages())
    print("")
    if not os.path.exists(db_connection_path):
        print("Creating", username, "connection:")
        print("-------------------" + "-" * (len(username) + 2))
        arcpy.CreateDatabaseConnection_management(azure_connections_dir,db_connection,"POSTGRESQL",instance,"DATABASE_AUTH",username,password,"SAVE_USERNAME",database)#,schema="#",version_type="TRANSACTIONAL",version="sde.DEFAULT",date="#")
        print(arcpy.GetMessages())
        print("")

    if prod and register_prod:
        register_with_datastore(db_connection_path)

    if ro_username is not None: # Use empty string '' ro_username to use add_ro_user default username with _ro at the end of the username
        add_ro_user(db_connection_path, ro_username, ro_password)

    return db_connection_path

# The following functions are primarily for use with copy_db_to_azure
def get_pg_credentials(host):
    username = on_prem_pg_user
    password = on_prem_pg_pw
    if connection_is_azure(host):
        username = pg_user
        password = pg_pw
    return username, password

def run_query(query, db, host, args=None):
    un, pw = get_pg_credentials(host)
    con = psycopg2.connect(database=db, user=un, password=pw, host=host)
    con.autocommit = True
    cur = con.cursor()
    if args:
        cur.execute(query, args)
    else:
        cur.execute(query)
    result = None
    try:
        result = cur.fetchall()
    except Exception as e:
        if str(e) != 'no results to fetch':
            print('Error:', e)
            print('Query was', query, args)
    cur.close()
    con.close()
    return result

# check whether a table has Replica Tracking Enabled
# only run query once to speed up script
class ReplicaTracking:
    """check which tables have Replica Tracking Enabled
    only run query once and store results to speed up script"""
    def __init__(self, db, host):
        sql = """SELECT table_name
                 FROM information_schema.columns
                 WHERE column_name='gdb_row_create_replica_guid';"""
                  # could also check for gdb_row_expire_replica_guid
        result = run_query(sql, db, host)
        self.tables = [r[0] for r in result]

    def enabled(self, table):
        return table in self.tables
def has_replica_tracking(table_or_tables, db, host, schema):
    """utility function to check a single table, but less efficient so not used for migration scripts"""
    if isinstance(table_or_tables, str): # allow for passing a single table name or a list of table names
        table_or_tables = [table_or_tables]
    tables = "','".join(table_or_tables)
    sql = f"""SELECT EXISTS (SELECT 1
             FROM information_schema.columns
             WHERE table_schema='{schema}' AND table_name in ('{tables}') AND column_name='gdb_row_create_replica_guid');"""
    # could also check for gdb_row_expire_replica_guid
    result = run_query(sql, db, host)
    return result[0][0]

def copy_db_to_azure(src_con, tgt_con, clear_target_db=True, test_mode=False, print_skipped_tables=True, matching_schema_only=False):
    """Copy a database from on-prem to Azure, including all feature classes, tables, and views
    Also copies the following table/dataset properties
    - traditional versioning (but does not create any versions or deal with branch versioning)
    - archiving
    - replica tracking

    Assumes that all feature classes and tables are registered with the geodatabase.
    For any non-sde tables. Copy those separately first.
    """

    import time

    target_ws = tgt_con
    source_host, source_db, source_un = get_connection_info(src_con)
    target_host, target_db, target_un = get_connection_info(tgt_con)
    ro_username = get_ro_username(tgt_con) # for clearing target locks prior to data updates
    yyyymmdd = time.strftime("%Y-%m-%d", time.localtime())
    log_path = os.path.join(connections_dir, 'migration_logs', f'{source_db}_{yyyymmdd}.log')
    rt = ReplicaTracking(source_db, source_host)

    # sync the versioning, archiving, and replica tracking settings on an object
    def sync_settings(t, target_path):
        desc = arcpy.Describe(t)
        tdesc = arcpy.Describe(target_path)
        if desc.isVersioned and not tdesc.isVersioned:
            try:
                if not test_mode:
                    arcpy.management.RegisterAsVersioned(target_path)
                print_and_log('\tVersioned')
            except Exception as e:
                print_and_log(f'\tError: failed to register as versioned: {e}')

        if 'Feature' in desc.datatype and desc.isArchived and not tdesc.isArchived: # isArchived not available for raster datasets
            try:
                if not test_mode:
                    arcpy.management.EnableArchiving(target_path)
                print_and_log('\tArchived')
            except Exception as e:
                print_and_log(f'\tError: failed to enable archiving: {e}')
        if desc.datatype == 'FeatureDataset': # Enable at the dataset level, but check fcs to determine need
            t = arcpy.ListFeatureClasses(feature_dataset=t)
        #if has_replica_tracking(t, source_db, source_host, source_un): # using ReplicaTacking class once instead of querying for each table is faster
        if rt.enabled(t):
            try:
                if not test_mode:
                    arcpy.management.EnableReplicaTracking(target_path) # hard to tell when this is needed
                print_and_log('\tReplicaTracked')
            except Exception as e:
                print_and_log(f'\tError: failed to enable replica tracking: {e}')

    f = open(log_path, 'w')
    def print_and_log(t):
        print(t)
        f.write(t + '\n')
        f.flush()

    print_and_log("Loading " + os.path.basename(tgt_con) + " from " + os.path.basename(src_con))

    try:
        # first clear out target schema
        t0 = time.time()
        if clear_target_db and not test_mode:
            terminate_user_connections(target_host, target_db, target_un)
            if ro_username:
                terminate_user_connections(target_host, target_db, ro_username)
            arcpy.env.workspace = target_ws

            datasets = arcpy.ListDatasets()
            for ds in datasets:
                try:
                    arcpy.management.Delete(ds)
                except Exception as e:
                    import traceback
                    print_and_log("Delete ds error")
                    print_and_log(str(e))

            tables = arcpy.ListFeatureClasses() + arcpy.ListTables()
            for table in tables:
                try:
                    arcpy.management.Delete(table)
                except Exception as e:
                    import traceback
                    print_and_log("Delete table error")
                    print_and_log(str(e))

            domains = [d.name for d in arcpy.da.ListDomains(target_ws)]
            for domain in domains:
                try:
                    arcpy.management.DeleteDomain(target_ws, domain)
                except Exception as e:
                    import traceback
                    print_and_log("Delete domain error")
                    print_and_log(str(e))

        t1 = time.time()
        if clear_target_db:
            print_and_log("Deleted: %s" % (t1-t0))

        # copy full db
        arcpy.env.workspace = src_con
        list_views_sql = """
        select table_schema as schema_name, table_name as view_name
        from information_schema.views
        where table_schema not in ('information_schema', 'pg_catalog', 'sde', 'public') and table_name NOT LIKE '%_evw'
        order by schema_name, view_name;"""
        view_tuples = run_query(list_views_sql, source_db, source_host)
        views = [v[1] for v in view_tuples]

        datasets = arcpy.ListDatasets()
        print_and_log(f'Datasets ({len(datasets)}):')
        for ds in datasets:
            ds = ds.split('.')[-1] # strip prefix
            if source_db == 'nat_wetlands_inv' and ds.startswith('archive'):
                continue
            target_path = os.path.join(target_ws, ds)
            if test_mode or not arcpy.Exists(target_path):
                print_and_log("COPY " + ds)
                if not test_mode:
                    try:
                        arcpy.Copy_management(ds, target_path)
                    except Exception as e:
                        print_and_log(f'\tWarning: failed to copy {ds} (Error: {e})')
                    else:
                        sync_settings(ds, target_path)
            elif arcpy.Exists(target_path):
                if print_skipped_tables:
                    print_and_log("SKIPPING " + ds)
                sync_settings(ds, target_path)

        tables = arcpy.ListFeatureClasses() + arcpy.ListTables()
        tables.sort(key=lambda t:'_' + t if '__attach' in t.lower() else t)
        print_and_log(f'Feature Classes and Tables ({len(tables)}):')
        for table in tables:
            schema, t = [p.lower() for p in table.split('.')[-2:]] # strip prefix #table.split('.')[-1].lower()
            if t in views: continue # skip views
            if matching_schema_only and schema != source_un:
                print_and_log("SKIPPING " + table + ", schema doesn't match")
                continue
            target_path = os.path.join(target_ws, t)
            if test_mode or not arcpy.Exists(target_path):
                print_and_log("COPY " + t)
                if not test_mode:
                    try:
                        arcpy.Copy_management(table, target_path)
                    except Exception as e:
                        print_and_log(f'\tWarning: failed to copy {table} (Error: {e})')
                    else:
                        sync_settings(table, target_path)
            elif arcpy.Exists(target_path):
                if print_skipped_tables:
                    print_and_log("SKIPPING " + t)
                sync_settings(table, target_path)
    except Exception as e:
        import traceback
        print_and_log("Delete or copy error")
        print_and_log(str(e))
        print_and_log(traceback.format_exc())
        sys.exit(1)

    t2 = time.time()
    print_and_log("Copied %s minutes" % round((t2-t1)/60,2))

    # The views may need to be created in the correct order so contingencies are created first
    # Get each view's dependencies & ddl and insert in the appropriate order into ddl_views
    ddl_views = [] # (view, ddl)#, contained_views)
    for view_tuple in view_tuples:
        ddl_sql = "select pg_get_viewdef('%s.%s');" % view_tuple
        ddl = "CREATE OR REPLACE VIEW %s.%s AS " % view_tuple + run_query(ddl_sql, source_db, source_host)[0][0]
        from_clause = ddl.split("FROM")[-1] # must be derived before adding alter table to get accurate contained_views
        ddl += "\nALTER TABLE {0}.{1} OWNER TO {0};".format(*view_tuple)
        contained_views = [v for v in views if v in from_clause and v != view_tuple[1]]
        i = 0
        for view in contained_views:
            for j, ddl_view in enumerate(ddl_views):
                if view == ddl_view[0]:
                    i = max(i, j + 1)
                    break
        ddl_views.insert(i, (view_tuple[1], ddl))

    # Create the views in the sorted order
    for view, ddl in ddl_views:
        print_and_log("Creating view " + view)
        if not test_mode:
            run_query(ddl, target_db, target_host + instance_suffix)

    t3 = time.time()
    if ddl_views:
        print_and_log("Added views %s" % (t3-t2))
    f.close()

def migrate_database(src_con, div='gen', create=True, tgt_db=None, tgt_user=None):
    """Migrate a postgres database from on-prem to Azure. Includes 2 major steps:
    1. Create new database in Azure (create_and_enable_azure_geodatabase)
    2. Copy data from on-prem to Azure (copy_db_to_azure)
    The data can also be reloaded if the database already exists by specifying create=False.
    Note that this will clear out everything in the target Azure database and copy everything fresh from on-prem.
    Arguments:
        src_con (required): the full source connection name (assumed to be somewhere in sde_connections) or path
        div (optional): defaults to the containing folder if it matches an instance, otherwise gen. Only used if create=True
        create (optional): defaults to True. Determines whether target db is created or assumed to exist
        tgt_db and tgt_user (optional): only needed to customize the db name and/or username and change it from the on-prem source
    typical use cases:
    migrate_database('ophost1-pg-db1.sde', 'gen') # Initial Create
    migrate_database('ophost1-pg-db1.sde', create=False) # Reload data
    migrate_database('ophost2-pg-db2.sde',tgt_db='db2_renamed',tgt_user='db2_renamed') # Rename database when migrating"""

    if not src_con.endswith('.sde'):
        src_con += '.sde'
    if not os.path.exists(src_con):
        cons = glob.glob(os.path.join(connections_dir, src_con)) + glob.glob(os.path.join(connections_dir, '*', src_con))
        if len(cons) == 1:
            src_con = cons[0]
        elif cons:
            print('Multiple connections match this name. Use the full path.')
            return
        else:
            print('No connections match this name. Please try again.')
            return

    host, database, username = get_connection_info(src_con)
    if not tgt_db:
        tgt_db = database
    else:
        print('Renaming database from', database, 'to', tgt_db)
    derived_div = get_connection_div(src_con) # Derive div and use that unless another one was provided
    if derived_div:
        if div in ('gen',derived_div,'derive',None): # if div was default or explicitly said to derive, override with no warning.
            div = derived_div
        else:
            print('Warning: specified instance', div, 'differs from the divisional folder where the sde was stored', derived_div)
    else:
        if not div:
            print('Error: Unable to derive division for', src_con, 'specify in migrate_database function call.')
            sys.exit(1)

    if create:
        clear_target_db = False
        # First verify user
        if tgt_user:
            print('Changing username from', username, 'to', tgt_user)
            actual_username = tgt_user
        else:
            arcpy.env.workspace = src_con
            tables = arcpy.ListFeatureClasses()
            if not tables:
                tables = arcpy.ListTables()
                if not tables:
                    tables = arcpy.ListDatasets()
            actual_username = tables[0].split('.')[1]
            if actual_username != username:
                print(f'Warning: username mismatch between connection file name ({username}) and database ({actual_username}).')
                #print('Rename source connection file')
                #return
        password = CI.get_password(actual_username)
        print('Creating New Azure DB with the following parameters:', div, tgt_db, actual_username, password)

        ro_username = get_ro_username(src_con)
        if ro_username:
            print('Also creating read-only username', ro_username)

        # 1. Create the target database
        tgt_con = create_and_enable_azure_geodatabase(div, tgt_db, actual_username, password, ro_username)
    else:
        clear_target_db = True
        azure_cons = glob.glob(os.path.join(azure_connections_dir, '*.sde'))
        cons_with_matching_db = [c for c in azure_cons if get_connection_info(c)[1] == tgt_db and not get_connection_info(c)[2].endswith('_ro')]
        if len(cons_with_matching_db) == 1:
            tgt_con = cons_with_matching_db[0]
        elif len(cons_with_matching_db) > 1:
            print(f"Multiple db matches in Azure: {cons_with_matching_db}\nRun copy_db_to_azure manually.")
            return
        else:
            print('No db matches in Azure. Please try again.')
            return

    # 2. Populate the target database from the source
    copy_db_to_azure(src_con, tgt_con, clear_target_db)
    return tgt_con
