'''
## Variables included are -  PATH TO HIVE-SITE.XML                                                                     #
Description:
The script used to read HIVE-SITE.XML file from cluster and pass necessary values to the
calling program

The RETURN values are used by SPARK  to connect HIVE database
##================================================================================================#
## REVISION HISTORY                                                                               #
##------------------------------------------------------------------------------------------------#
## 2019-09-26:: v1.0  :: Sambit Baliarsingh     :: Created                                        #
##================================================================================================#
'''
def get_hive_param():
    hivexml=sys.argv[1]
    mytree=xml.etree.ElementTree.parse(hivexml)
    myroot=mytree.getroot()
    settings_var=""
    if __name__ == "__main__":
        try :

            for x in myroot.findall('property'):
                try:
                    name=x.find('name').text
                    value=x.find('value').text
                    #print(name, value)
                    if name == 'hive.metastore.authorization.storage.checks':
                        settings_var='{}(@{}@,@{}@)|'.format(settings_var,name,value)
                    elif name=='hive.exec.dynamic.partition.mode':
                        xval = '(@{}@,@{}@)|'.format(name,value)
                        settings_var=settings_var+xval
                    elif name == 'hive.metastore.cache.pinobjtypes':
                        xval = '(@{}@,@{}@)|'.format(name, value)
                        settings_var = settings_var + xval
                    elif name == 'hive.metastore.client.connect.retry.delay':
                        xval = '(@{}@,@{}@)|'.format(name, value)
                        settings_var = settings_var + xval
                    elif name == 'hive.metastore.client.socket.timeout':
                        xval = '(@{}@,@{}@)|'.format(name, value)
                        settings_var = settings_var + xval
                    elif name == 'hive.metastore.connect.retries':
                        xval = '(@{}@,@{}@)|'.format(name, value)
                        settings_var = settings_var + xval
                    elif name == 'hive.metastore.execute.setugi':
                        xval = '(@{}@,@{}@)|'.format(name, value)
                        settings_var = settings_var + xval
                    elif name == 'hive.metastore.failure.retries':
                        xval = '(@{}@,@{}@)|'.format(name, value)
                        settings_var = settings_var + xval
                    elif name == 'hive.metastore.kerberos.keytab.file':
                        xval = '(@{}@,@{}@)|'.format(name, value)
                        settings_var = settings_var + xval
                    elif name == 'hive.metastore.kerberos.principal':
                        xval = '(@{}@,@{}@)|'.format(name, value)
                        settings_var = settings_var + xval
                    elif name == 'hive.metastore.pre.event.listeners':
                        xval = '(@{}@,@{}@)|'.format(name, value)
                        settings_var = settings_var + xval
                    elif name == 'hive.metastore.sasl.enabled':
                        xval = '(@{}@,@{}@)|'.format(name, value)
                        settings_var = settings_var + xval
                    elif name == 'hive.metastore.server.max.threads':
                        xval = '(@{}@,@{}@)|'.format(name, value)
                        settings_var = settings_var + xval
                    elif name == 'hive.metastore.uris':
                        xval = '(@{}@,@{}@)|'.format(name, value)
                        settings_var = settings_var + xval
                    elif name == 'hive.metastore.warehouse.dir':
                        xval = '(@{}@,@{}@)|'.format(name, value)
                        settings_var = settings_var + xval
                    elif name=='hive.exec.dynamic.partition':
                        xval = '(@{}@,@{}@)|'.format(name, value)
                        settings_var = settings_var + xval
                except:
                    continue
                file_path = settings_var.rstrip(',')
        except:
            file_path="Problem in Hive FIle Reading"
    return file_path
def main():
    hive_string=get_hive_param()
    print(hive_string)

if __name__ == "__main__":
    import sys
    import xml.etree.ElementTree
    main()
