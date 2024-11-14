# eMobility_CaseStudy_OpenChargeMap
This repository contains all the azure databricks notebook required for the end-to-end completion of Open Charge Map case study.

The case study revolves around utilizing OpenChargeMap's extensive data about EV charging infrastructure.
Using the data we first store the data into ADLS gen 2 storage containers.

Then we process the data using medallion architecture (Bronze (RAW), Silver (Enriched) and Gold (Curated) layers).

There is a workflow present in the Azure databricks named "OpenChargeMap_CaseStudy_Sparsh", This workflow will automatically trigger each notebook sequentially and will extract data from OCM's server and will process the data till Silver layer.
The only additional dependency used in this case study is "tzfpy==0.16.0", it is used to find out the location of a place using its latitude and longitude.
