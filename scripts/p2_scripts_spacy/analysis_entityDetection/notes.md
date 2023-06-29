## Notes 

- Pull down S3 file (json) - individual file 
- Analyze it for entity detection; perhaps begin with SDoH related terms
- After performing the entity detection, add those new fields to it - back into JSON 
- Push it back up to S3 
- Once completed, the AWS crawler should then re-analyze, update Athena to contain the new fields 