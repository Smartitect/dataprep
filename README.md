# Introduction

[Hymans Robertson](https://hymans.co.uk) are a 100-year old company providing independent pensions, investments, benefits & risk consulting services, data & tech solutions to employers, trustees & financial services institutions, regulated by the Financial Conduct Authority in the UK.

Since 2013, [endjin](https://endjin.com) have been [helping Hymans transform](https://endjin.com/industries/financial-services#hymans-robertson); initially by helping them migrate to Azure (and overcoming regulatory hurdles as they were the first UK FS customer to put PII data into Azure), next by helping to create an innovation process to enable them to start investing in product R&D to help the firm to digitally transform its service offerings.

## Background

This experiment focuses on the "TPA (Third Party Administration) Installations" process. This occurs whenever we win a new a (pension) scheme and involves the transfer of data from the outgoing (or "ceding") administrator to Hymans Robertson.

The process currently takes *8 weeks end-to-end*. It is a very serial process - i.e. Hymans can't get a detailed handle on data quality until 6-weeks into the 8-week process.

The industry is skewed such that we run this process at a loss on the premise of making a return over the long term by administering the (pension) scheme.

The process also acts as a bottleneck. Hymans can only support 10 to 12 installations per annum at full throttle. The company would love to be able to do more and therefore win more clients: the market is "coming to us" at the moment given the poor performance of other administrators and the strategic move from others to withdraw from the market (due to regulatory disruption).

Poor data quality is rife in the industry. The party producing the data extract is doing so as part of project shut down after a commercial loss. This often means that a "quick and dirty" approach is taken, with no motivation to improve. So Hymans need to be able to work with ambiguity / gaps in the data, without seeking remedy at the source.

## Vision

If Hymans can crack this problem it could be transformational for the company. Hymans could:

- win more business (grow revenue) and make that business more profitable (increase profit margins) - in an industry where domination is becoming increasingly important, so there are significant "gold bars" downstream if we can increase market share
- leverage the capability wider (new revenue streams) - e.g. offering data cleanse services to our competitors, or plugging this capability into wider services that involve data cleansing for administration data such as "buy-outs".
- leverage the underlying technology and tools in other areas where we are handling similar data such as Club Vita.

# Design Principles

1. The overarching requirement is that **Hymans must not loose any transactions as part of this process** - if we do, people won't get paid their pensions!  So the ability to reconcile raw data received from the ceding administrator with the final data set we load into our system is important.  This includes simple checks like transaction counts but also more complex checks such as summing up financial amounts to check they balance "to the penny".
2. We get data from a number of different organisations using a relatively small set of off the shelf administration platforms.  Therefore the data we receive is broadly in one of a small number of recognisable formats.  However this varies - based on the version of the underlying administration platform AND based on the way it has been configured set up for that administrator AND based on the way it has been configured for that particular scheme. But we anticipate that we can build up a set of **reusable "recipes"** for each of these cases that can be tweaked quickly based on these variations.
3. The current process takes about **8 weeks**, we'd love to transform that - reduce it to days rather than weeks, make it profitable in its own right, use some of the savings to put more effort into data quality with massive downstream benefits as we move increasingly to on-line administration - i.e. pension scheme members access services over web rather than phone / letter / email.
4. The technical rigour around the production of data files can be variable, leading to us receiving **poor quality data** (e.g. unquoted commas in a CSV file), so we need to be able cope with this without having to do manipulation manually or outside the platform.
5. Ability to **define canonical forms** on which to map data so that we can make downstream tasks common.
6. Defining a **"meta process"** so that we can logically separate the different stages in the process: e.g. ingestion -> data integrity checks -> map to canonical form -> data type checks -> more complex data quality logic.
7. Ability to orchestrate all of the above as part of a **modern, open cloud-based architecture** (i.e. data encrypted by default using BYOK, orchestrated by Azure Data Factory, serverless or pay per transaction).
8. Further to the point above, the ability to **perform data quality checks at differing levels of complexity**, ranging simple data type checks (e.g. valid Postcode/ZIP code) to logic on a single transaction (e.g. date of birth earlier than date joined scheme) through to more complex checks that involve aggregation across multiple transactions/rows/members.
9. In parallel Hymans have been working with Azure Cognitive Services to strip data out of scanned documents and handwritten notes (the unstructured data that accompanies the structured data we are focusing on here). Ultimately, we would like to do "fuzzy matching" to strip data out of these documents to enable us to plug gaps / improve data quality in the structured data. One broader goal to bear in mind at this point.

# Example Scenarios

- Dealing with poor quality raw data (e.g. commas in address column skewing CSV import)
- Intelligent mapping to "canonical form"
- Spotting anomalies in the data (e.g. date of birth defaults used "1/1/1971")
- Gender is not Male or Female / Incorrect Gender for members title
- Missing Invalid or Temporary NI Number
- Missing Addresses / Check for commas in members address / Missing postcodes
- Date Joined Company is after Date Joined Scheme
- Date of Birth is after Date Joined Scheme / Missing Invalid or known false Date of Birth
- Missing Scheme Retirement Date

## Scenarios joining tables, calculated basis, etc.

- Members annual pension should total sum of pension elements
- Members annual pension not divisible by 12

## Initial questions

- Repeating a recipe/looping
- Using regular expressions/creating custom data expressions
- Using union to join datasets
- Best practices for naming conventions
- Interacting dynamically with the graphical representation to see the subset of data either passing or failing tests?

## Data Integrity, Validation Rules & Data Types

NOTE: Table data is linked by `KeyObjectIDs` i.e. `PEOPLE.ID = MEMBERS.PEOPLEID`, `MEMBERS.MEMNO = SERVICE.MEMNO` etc.

UK Postcode: to be applied to the `PEOPLE.POSTCODE` column:

```regexp
([Gg][Ii][Rr] 0[Aa]{2})|((([A-Za-z][0-9]{1,2})|(([A-Za-z][A-Ha-hJ-Yj-y][0-9]{1,2})|(([A-Za-z][0-9][A-Za-z])|([A-Za-z][A-Ha-hJ-Yj-y][0-9][A-Za-z]?))))\s?[0-9][A-Za-z]{2})
```

From: [stackoverflow](https://stackoverflow.com/questions/164979/uk-postcode-regex-comprehensive)

UK National Insurance Number: to be applied to the `PEOPLE.NINO` column:

```regexp
^([a-zA-Z]){2}( )?([0-9]){2}( )?([0-9]){2}( )?([0-9]){2}( )?([a-zA-Z]){1}?$
```

From: [stackoverflow](https://stackoverflow.com/questions/10204378/regular-expression-to-validate-uk-national-insurance-number)

Show validation of the `PEOPLE.TITLE` column against a list based type (`SEX` for data quality check below):

|    TITLE   |     SEX       |
|------------|---------------|
|    Mrs     |    Female     |
|    Mr      |    Male       |
|    Miss    |    Female     |
|    Ms      |    Female     |
|    Dr      |    Unknown    |
|    Rev     |    Unknown    |
|    Sir     |    Male       |
|    Dame    |    Female     |

Create a recipe that returns members (i.e. `Surname`, `NINO` ) who have a record present in the `EXITDEFERREDS` dataset with no corresponding entry in `STATHIS` where the `SSTA = 15`. The join between `EXITDEFERREDS` and `STATHIS` will need to incorporate the `MEMBERS & PEOPLE` datasets as these contains the link between the other two tables (For reference, `MEMNO` which is the "Common ID" between the three tables & `PEOPLE.ID = MEMBERS.PEOPLEID`, `MEMBERS.MEMNO = EXITDEFERREDS.MEMNO` etc.) This should return a subset of members without the correct entry in the `STATHIS` table showing us where Status History issues exist within the data.

This recipe should be easily adaptable to be used against the `EXITREFUND` and `EXITRETIREMENT` datasets to be used with different `SSTA` values. These `SSTA` values are contained within the `LOOKUPS` dataset but are not easily identifiable by a specific `KeyObjectID` or relevant column header.  Hence we have specified the value 15 for deferreds to make life easier for the recipe above. It would be interesting to see if there is a way to link or define a subset of the `LOOKUPS` table or perform matching via machine learning.

If Date of Birth is after Date Joined Scheme / Missing Invalid or known false Date of Birth:

- New calculated column that flags when `PEOPLE.DOB` is greater than `MEMBERS.DJS` (see dataset join above)
- Also, calculate number of days between `DOB` and `DJS` (where negative numbers would allow us to spot other potential issues)

# Installation Instructions

Install a 64bit version of Python from https://www.python.org/downloads/windows/

Install VS Code and the Python Extension https://marketplace.visualstudio.com/items?itemName=ms-python.python

Install the DataPrep SDK via `pip install --upgrade azureml-dataprep`