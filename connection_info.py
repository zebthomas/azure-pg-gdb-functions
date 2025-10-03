host_names = ['hostgen','host001','host002']
instance_suffix = "-psql-001.postgres.database.azure.com"
prod_instance_prefix = "r29p-"
test_instance_prefix = "r29t-"
sde_pw = 'sdepass'
pg_user = 'postgresadmin'
pg_pw = 'pgpass'
on_prem_pg_user = 'postgres'
on_prem_pg_pw = 'pgpass'
ags_admin_connections = ['https://xxx.state.mn.us/server1/admin','server2 on xxx.state.mn.us.ags')]

def get_password(username, ro=False):
    password = 'password'
    if ro: password += '_ro'
    return password
