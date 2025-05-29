import os
from arcgis.gis import GIS

gis = GIS(
    'https://governmentofbc.maps.arcgis.com', 
    os.getenv('AGO_USERNAME_BP'), 
    os.getenv('AGO_PASSWORD_BCPARKS'), 
    verify_cert=False
) 


me = gis.users.me

# print basic account info
print(f"Username:         {me.username}")
print(f"Role:             {me.role}")              
print(f"User Type:        {me.userType}")    
print(f"License Type ID:  {me.userLicenseTypeId}")


privileges = gis.users.me.privileges
print(f"\n{me.username} Account Privileges:")
for privilege in sorted(privileges):
    print(f"  {privilege}")


for folder in me.folders:
    title = folder['title']
    fid   = folder['id']
    print(f"Folder: {title}")

    # when we hit AMS Data, list title, type, and owner of each item
    if title == 'AMS Data':
        items = me.items(folder=fid, max_items=500)
        print("  Items in 'AMS Data':")
        for itm in items:
            print(f"   • {itm.title} ({itm.type}) – Owner: {itm.owner}")
