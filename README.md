# randomdownloadchecker
Lambda for attempting random downloads. This would be EXCEPTIONALLY easy to wrap up in a cloudformation object, but I don't have the time to do that right now. Feel free to push a CF template as a MR. 

## Setup

### Upload lambda_function.py 
However you want to upload... 

### Create SNS
The script uses an SNS to relay success/falure. Create an SNS topic for the script to use

### Add Lambda Environment Variables
SNS Topic ARN:
```
sns_arn = arn:aws:sns:us-east-1:777766665555:RandomDownloadChecker
```

Which CMR to query:
```
cmr_api = https://cmr.earthdata.nasa.gov
```

URS Username and Password:
```
urs_user = valid_urs_user
usr_pass = SomeCr@zyPassword
```

Collections to avoid trying to download from (comma seperated list):
```
skip_collections = C1234567-DAAC,C765432-DAAC,C55555555-DAAC
```

More complex CMR search filtering parameters (get params):
```
collection_filter = provider=ASF
```

### Create a Cloudwatch Event
Set up a scheduled event to run the lambda at whatever periodicity you desire 

### Sit back and bask in the glory that is piece of mind knowing your downloads are successful!
