import os
from arcgis.gis import GIS

gis = GIS(
    'https://governmentofbc.maps.arcgis.com', 
    os.getenv('AGO_USERNAME_ML'), 
    os.getenv('AGO_PASSWORD_ML'), 
    verify_cert=False
) 


me = gis.users.me

# print basic account info
print(f"Username:         {me.username}")
print(f"Role:             {me.role}")              
print(f"User Type:        {me.userType}")    
print(f"License Type ID:  {me.userLicenseTypeId}")

'''
privileges = gis.users.me.privileges
print(f"\n{me.username} Account Privileges:")
for privilege in sorted(privileges):
    print(f"  {privilege}")
'''