Scraper and mongodb inserter for 2019 NCAA stats.

Predecessor code: ncaalinks_hotfix

mongodb: stageprod19

newcode: the incremental code to the predecessor ncaalinks_hotfix is imputed elements to the document of week_no and week_ended. Additionally, if the stats category collection already exists in the db, ensure to append documents to the collection and not overwrite. 

Planned db schema design:
db: stageprod19 -> teams19

within stageprod19:
One collection per statistical category
within the collection: One document per team per week
Anticipated documents in each collection: 129 (teams) * 13 weeks (of CFB)
