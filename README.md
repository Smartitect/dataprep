# Data quality scenarios:

| Requirement | Achieved? | Notes |
| --- | --- | --- |
| Dealing with poor quality raw data (e.g. commas in address column skewing CSV import) | No |  |
| Intelligent mapping to "canonical form"  | No |  |
| Spotting anomalies in the data (e.g. date of birth defaults used "1/1/1971") | No |  |
| Gender is not Male or Female / Incorrect Gender for members title | No |  |
| Missing Invalid or Temporary NI Number | No |  |
| Missing Addresses / Check for commas in members address / Missing postcodes | No |  |
| Date Joined Company is after Date Joined Scheme | No |  |
 Date of Birth is after Date Joined Scheme / Missing Invalid or known false Date of Birth | No |  |
| Missing Scheme Retirement Date  | No |  |

# Data reconciliation scenarios:

[] Members annual pension should total sum of pension elements
[] Members annual pension not divisible by 12

# Capabilities that we want to highlight:

[] Repeating a recipe / looping to apply particular operations
[] Using regular expressions/creating custom data expressions
[X] Using union to join datasets
[] Best practices for naming conventions
[] Interacting dynamically with the graphical representation to see the subset of data either passing or failing tests?

# Specific examples of data types / data quality checks

## Validation agasint regular expressions

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

## Validation using a look-up table

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


Table data is linked by `KeyObjectIDs` i.e. `PEOPLE.ID = MEMBERS.PEOPLEID`, `MEMBERS.MEMNO = SERVICE.MEMNO` etc.

Create a recipe that returns members (i.e. `Surname`, `NINO` ) who have a record present in the `EXITDEFERREDS` dataset with no corresponding entry in `STATHIS` where the `SSTA = 15`. The join between `EXITDEFERREDS` and `STATHIS` will need to incorporate the `MEMBERS & PEOPLE` datasets as these contains the link between the other two tables (For reference, `MEMNO` which is the "Common ID" between the three tables & `PEOPLE.ID = MEMBERS.PEOPLEID`, `MEMBERS.MEMNO = EXITDEFERREDS.MEMNO` etc.) This should return a subset of members without the correct entry in the `STATHIS` table showing us where Status History issues exist within the data.

This recipe should be easily adaptable to be used against the `EXITREFUND` and `EXITRETIREMENT` datasets to be used with different `SSTA` values. These `SSTA` values are contained within the `LOOKUPS` dataset but are not easily identifiable by a specific `KeyObjectID` or relevant column header.  Hence we have specified the value 15 for deferreds to make life easier for the recipe above. It would be interesting to see if there is a way to link or define a subset of the `LOOKUPS` table or perform matching via machine learning.

If Date of Birth is after Date Joined Scheme / Missing Invalid or known false Date of Birth:

- New calculated column that flags when `PEOPLE.DOB` is greater than `MEMBERS.DJS` (see dataset join above)
- Also, calculate number of days between `DOB` and `DJS` (where negative numbers would allow us to spot other potential issues)

# Installation Instructions

Install a 64bit version of Python from https://www.python.org/downloads/windows/

Install VS Code and the Python Extension https://marketplace.visualstudio.com/items?itemName=ms-python.python

Install the DataPrep SDK via `pip install --upgrade azureml-dataprep`