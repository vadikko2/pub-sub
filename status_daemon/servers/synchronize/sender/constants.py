"""GQL запросы к сервису int-configs"""

GET_HOSTS_GQL_QUERY = '''
{
  getTrustedServers{
    host
  }
}
'''

GET_SERVER_NAME = '''
{
    getServerName{
        host
    }
}
'''
