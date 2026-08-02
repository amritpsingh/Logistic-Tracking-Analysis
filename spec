Build Module for processing household and individual names

Census methodology specification 

Clean names for linking 

 

Descriptions 

This specification is used for parsing names for linking IF/HSF purpose on the following two tables: 

Census Individual frame 

Census dwelling person frame.  

The idea is that we will tidy up names by removing titles and any accented or special characters. Names are first concatenated, then in case where the first and middle names are joined together with no spaces, a lookup is performed using a name lookup table which is a list of common first names to see if a first name can be extracted from the concatenation of first and middle names. 

 

2025 field test background: 

1 - Names are copied from the HSF to “If _piped names” - variable IF_piped_names: {"family": "Stag GL", "first": "Triumph"}  

and “IF_name” - variable IF_name: {"1": "Triumph", "2": "Stag GL"} 

 

2 - If the respondent says yes to confirm the names are correct – variable IF_names_confirmed: "yes" then these are the only name fields.  

 

3 - If they say no – variable IF_names_confirmed: "no" then there is another name field “IF_names”- variable IF_names: {"family_name": "Stag GL", "first_names": "Triumph"} 

 

4 - If the unit is not linked or the access code redelivered flag is set then masking occurs and the respondent is asked to enter their names on the Individual Form. “IF_names” is populated - variable IF_names: {"family_name": "Stag GL", "first_names": "Triumph"} The variable IF_names_confirmed: does not appear in the payload because this is not asked in this scenario. 

 

5. When the individual data is parsed: 

IF_name is mapped to ‘i_first_name_hsf’ and ‘i_family_name_hsf’ 

IF_names is mapped to ‘i_first_name and ‘i_family_name’ 

 

 

 

 

 

For Census Individual table 

 

Input Table Information 
Operational Service: OS10 
Workspace: Survey Data Sourcing 
Catalog: socpophousdata1_<env>  
Schema: social_census_validated 
Table: individual 

 

Output Table Information 
Operational Service: OS10 
Workspace: Survey Data Sourcing 
Catalog: socpophousdata1_<env>  
Schema: social_census_names_processed 

Table: individual_names 

 

Input variables 

Variable Name  

Format 

Classification  

Person_nbr 

string 

N/A 

i_first_name_hsf 

string 

N/A 

i_family_name_hsf 

string 

N/A 

i_name_confirmed 

string 

N/A 

i_first_name 

string 

N/A 

i_family_name 

string 

N/A 

 

Output variables 

Variable Name  

Format 

Classification  

Person_nbr 

String 

N/A 

i_parsed_first_name 

string 

N/A 

i_parsed_last_name 

string 

N/A 

i_parsed_first_name_full 

string 

N/A 

 

Rules 

The order of steps is as follows: 

If i_name_confirmed = "yes" 

Concatenate the i_first_name_hsf and i_family_name_hsf arguments together, delimiting with a whitespace character. 

Else i_name_confirmed = "no" or missing 

Concatenate the i_first_name and i_family_name arguments together, delimiting with a whitespace character. 

 

On the concatenated names: 

Reduce accented characters to their ascii components 

Transform all lowercases into uppercases 

Remove these common titles: mr, miss, mrs, ms, dr, wife, jnr, jr 

Remove the word “husband” if it appears at the beginning but keep it if it appears at the end of the concatenated name 

Remove anything within bracket (), and bracket itself 

Remove any special characters like $%^@!&*#?, hyphen and space between hyphens, or in other words, remove anything which is not the alphabet letters a-z or ‘s 

Concatenate multi-part surnames. If the following prefixes occur in the name field, and there is further text following them, then drop the white space to the right of the prefix to concatenate with the rest of name : 

o, mc, mac, te, de, da, van, von, den, der, le, la, al, el, fa, cos, del, des, di, du  

Remove leading and trailing white space, and replace any 2 or more spaces within the text with a single space 

Split the concatenated name into a list of names by space and take the last element as the i_parsed_last_name and the other part as the i_parsed_first_name_full. Take the first element of i_parsed_first_name_full as the i_parsed_first_name.  

 

 

 

 

 

 

 

 

 

 

 

 

 

 

 

 

 

 

For Census Household person table 

 

Input Table Information 
Operational Service: OS10 
Workspace: Survey Data Sourcing 
Catalog: socpophousdata1_<env>  
Schema: social_census_validated 
Table: dwelling_person 

 

Output Table Information 
Operational Service: OS10 
Workspace: Survey Data Sourcing 
Catalog: socpophousdata1_<env>  
Schema: social_census_names_processed 
Table: household_names 

 

Input variables 

Variable Name  

Format 

Classification  

dwell_nbr 

String 

N/A 

first_names 

string 

N/A 

family_name 

string 

N/A 

 

Output variables 

Variable Name  

Format 

Classification  

d_person_nbr 

string 

N/A 

d_parsed_first_name 

string 

N/A 

d_parsed_last_name 

string 

N/A 

d_parsed_first_name_full 

string 

N/A 

 

Rules 

Repeat everything in the rules above, replace the fields in the table below:  

Replace this: 

With this: 

i_parsed_first_name 

d_parsed_first_name 

i_parsed_last_name 

d_parsed_last_name 

i_parsed_first_name_full 

d_parsed_first_name_full 

 
