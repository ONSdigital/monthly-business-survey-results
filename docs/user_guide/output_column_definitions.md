<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th style="text-align:left">Output column</th>
      <th style="text-align:left">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="text-align:left">adjustedresponse</td>
      <td style="text-align:left">A numeric column representing processed response from a contributor that is used to generate survey results. See SPP responses documentation to see how this variable is adjusted in the responses pipeline. In the results pipeline if this value is updated it is firstly copied into a new column so that the original value is preseved. For example "pre_constrained_adjustedresponse" is a copy of the "adjustedresponse" column before the constraint rules are applied to derived questions.<br>Data type: float or int (dependng on the survey data and processing)</td>
    </tr>
    <tr>
      <td style="text-align:left">adjustedresponse_man</td>
      <td style="text-align:left">This variable contains the manual construction values provided by the user in a manual construction csv file. It enables manual corrections to specific imputes in cases where domain expertise disagree with automatic imputation.<br>Data type: Matches the data type of the target variable (float, int, or string, depending on target)</td>
    </tr>
    <tr>
      <td style="text-align:left">b_link_adjustedresponse</td>
      <td style="text-align:left">Backward imputation link for an observation, used for backward imputation. Backward imputation imputes data for non-responders in the target period by multiplying a link to the predictive period data, where the predictive period is the period that immediately follows the target period (i.e. consecutive period). Backward imputation from construction does not occur. Records imputed using this imputation link will be marked bir.<br><br>The backward link is a ratio within each imputation class, defined as the sum of the target period's matched pair responses divided by the sum of the next period's matched pair responses. In the case where the backward imputation link cannot be calculated, e.g. previous period is not present for that observation or if it is zero, the backward link defaults to 1.<br><br>For forward and backward link calculations, only contributors that have responded in both the target and predictive period and are in the same imputation class for both periods shall be used to calculate the ratios.</td>
    </tr>
    <tr>
      <td style="text-align:left">b_match_filtered_adjustedresponse</td>
      <td style="text-align:left">A boolean variable that returns True if the contributor is a backward matched pair, i.e. has returned cleared values for both the target period and following period, and False if not. The contributor must also have been a member of the same imputation class for both periods to be a valid matched pair.</td>
    </tr>
    <tr>
      <td style="text-align:left">b_match_filtered_adjustedresponse_count</td>
      <td style="text-align:left">The number of observations in a given imputation class that have returned in both the observation period and following period.</td>
    </tr>
    <tr>
      <td style="text-align:left">backdata_adjustedresponse</td>
      <td style="text-align:left">A column containing the original vlaue of the `adjustedresponse` variable for each record in the back data period. The column preserves the original value of the target variable for the back data period, before any imputation or modification occurs.<br>Data type: Same as the target variable (commonly float, int depending on the data)</td>
    </tr>
    <tr>
      <td style="text-align:left">backdata_flags_adjustedresponse</td>
      <td style="text-align:left">A string column indicating the imputation flag for each record in the back data period, specially for the `adjustedresponse` target variable. The value in this column represents the type of imputation or response status for each record in the back data period. It takes the following values:<br><br>fir :      Forwards imputation from response<br>fic :      Forwards imputation from construction<br>fimc :   Forwards imputation from manual construction<br>bir :      Backwards imputation from response<br>c :         Construction imputation using auxiliary variable<br>mc :     Manual construction<br>r :         Response</td>
    </tr>
    <tr>
      <td style="text-align:left">calibration_factor</td>
      <td style="text-align:left">The calibration factor is an additional estimation weight using auxiliary data from the business register for improved estimation. For a given calibration group, it is a ratio of the sum of the auxiliary variable for the population over the sum of the weighted auxiliary variable for the sample. It is given as a floating point.</td>
    </tr>
    <tr>
      <td style="text-align:left">calibration_group</td>
      <td style="text-align:left">Variable for the grouping used in the calculation of the calibration factor.</td>
    </tr>
    <tr>
      <td style="text-align:left">cell_no</td>
      <td style="text-align:left">Cell number provided by IDBR. Each unique cell number represents a sampling stratum used in the survey design.</td>
    </tr>
    <tr>
      <td style="text-align:left">classification</td>
      <td style="text-align:left">An industry based grouping of businesses, similar to the first three digits of the Standard Industrial Classification. The model that generates L values for winsorisation, which is outside the scope of this pipeline, groups businesses by classification. Therefore L values are provided by Methodology by classification and question number. It is given as an integer.<br>Source: The "classification" is loaded from the file specified by "classification_values_path" in the join_l_values function.</td>
    </tr>
    <tr>
      <td style="text-align:left">constrain_marker</td>
      <td style="text-align:left">A string variable stating how a value for a question has been derived according to its SPP form ID. It is based on the following ruleset: <br> - For form ID 13, question number 40 is created by summing 46,47 (constrain_marker = "sum[46, 47]")<br> - For form ID 14, question number 40 is created by summing 42,43 (constrain_marker = "sum[42, 43]")<br> - For form ID 15, question number 46 is created from 40 (constrain_marker = "sum[40]"), question number 47 with derived value of 0 is also created from 40. (constrain_marker = "Zero for winsoration")<br> - For form ID 16, question number 42 is created from 40 (constrain_marker = "sum[40]"), question number 43 with derived value of 0 also created (constrain_marker = "Zero for winsorisation")<br><br>In addition for all form types (when question number is available):<br> - 40 is replaced with 49 when 49 &gt; 40. (constrain_marker = "49 &gt; 40")<br> - 40 is replaced with with 90 when 90 &gt;= 40. (constrain_marker = "90 &gt;= 40")</td>
    </tr>
    <tr>
      <td style="text-align:left">construction_link</td>
      <td style="text-align:left">The construction link for an observation, used for construction imputation. Construction imputation imputes data for non-responders in the target period using an auxiliary variable from the target period  multiplied by a link to create a constructed value. Records imputed using this imputation will be marked c.<br><br>The construction link is a ratio within each imputation class, the sum of the matched pair responses in the target period divided by the sum of the matched pair responders' auxiliary values for the target period. If the case that the sum of auxiliary values is zero, the construction link defaults to 1.<br><br>For construction link calculations, only contributors that have responded in the target period and have a valid auxiliary variable shall be used to calculate the ratio.</td>
    </tr>
    <tr>
      <td style="text-align:left">converted_frotover</td>
      <td style="text-align:left">The frozen turnover of a business from the IDBR converted from annual pounds-thousands into monthly actual pounds.</td>
    </tr>
    <tr>
      <td style="text-align:left">default_link_b_match_adjustedresponse</td>
      <td style="text-align:left">A boolean variable that returns True if the backwards link has been defaulted to 1 and False otherwise.</td>
    </tr>
    <tr>
      <td style="text-align:left">default_link_f_match_adjustedresponse</td>
      <td style="text-align:left">A boolean variable that returns True if the forwards link has been defaulted to 1 and False otherwise.</td>
    </tr>
    <tr>
      <td style="text-align:left">default_link_flag_construction_matches</td>
      <td style="text-align:left">A boolean variable that returns True if the construction link has been defaulted to 1 and False otherwise.</td>
    </tr>
    <tr>
      <td style="text-align:left">design_weight</td>
      <td style="text-align:left">The design weight of a given business sampled from a given stratum is 1 divided by the probability of selection for the survey. It is calculated using the following ratio: The population size of the given stratum divided by the sample size of the given stratum. The design weight is given as a floating point.</td>
    </tr>
    <tr>
      <td style="text-align:left">entname1</td>
      <td style="text-align:left">The name of the Enterprise associated with the Reporting Unit, or reference. This comes from IDBR</td>
    </tr>
    <tr>
      <td style="text-align:left">f_link_adjustedresponse</td>
      <td style="text-align:left">The forward imputation link for an observation, used for forward imputation. Forward imputation imputes data for non-responders in the target period by multiplying a link to the predictive data, where the predictive period is the period that immediately precedes the target period. It should be noted that the same link is used for forwards imputation from a response (fir), a constructed value (fic) and a manually constructed value (fimc).<br><br>The forward imputation link is unique for a given period within the imputation class, defined as the sum of the target period's matched pair responses divided by the sum of the previous period's matched pair responses. In the case where the sum of the previous period's matched pair responses cannot be calculated (e.g. no matched pair responses in the previous period) the forward link defaults to 1.<br><br>For forward and backward link calculations, only contributors that have responded in both the target and predictive period (i.e. matched pairs) and are in the same imputation class for both periods shall be used to calculate the links.</td>
    </tr>
    <tr>
      <td style="text-align:left">f_match_filtered_adjustedresponse</td>
      <td style="text-align:left">A boolean variable that returns True if the contributor is a forward matched pair, i.e. has returned cleared values for both the target period and previous period, and False if not. The contributor must also have been a member of the same imputation class for both periods to be a valid matched pair.</td>
    </tr>
    <tr>
      <td style="text-align:left">f_match_filtered_adjustedresponse_count</td>
      <td style="text-align:left">The number of observations in a given imputation class that have returned in both the observation period and previous period.</td>
    </tr>
    <tr>
      <td style="text-align:left">filtered_adjustedresponse</td>
      <td style="text-align:left">A column representing the `adjustedresponse` values to feed into imputation link calculations after applying row-level filters to exclude certain records provided by the user from the link calculations.<br>Data type: Same as the target variable, (typically float or int, with possible NaN values)</td>
    </tr>
    <tr>
      <td style="text-align:left">flag_construction_matches_count</td>
      <td style="text-align:left">This is a count of the observations that have a "Clear" or "Clear - overridden" status and a valid auxiliary variable in the observation period</td>
    </tr>
    <tr>
      <td style="text-align:left">form_type_spp</td>
      <td style="text-align:left">This variable is the encoding used by the Statsitical Production Platform to describe the Form ID.</td>
    </tr>
    <tr>
      <td style="text-align:left">formtype</td>
      <td style="text-align:left">The type of form sent out to a contributor. This variable is sourced from IDBR. A mapping of question number to form type is provided in SPP documentation on Confluence</td>
    </tr>
    <tr>
      <td style="text-align:left">froempment</td>
      <td style="text-align:left">A variable for employment of the reference, sourced from the IDBR. It is used as a part of sample design as criteria to classify business size as part of the sampling method.<br>Data type: int</td>
    </tr>
    <tr>
      <td style="text-align:left">frosic2007</td>
      <td style="text-align:left">Frozen Standard Industrial Classification (SIC) 2007 code for the reporting unit, or reference.  Used as a basis to define stratum, or cells, for sample selection.<br>Source: IDBR<br>Data type: string</td>
    </tr>
    <tr>
      <td style="text-align:left">frotover</td>
      <td style="text-align:left">The frozen turnover of the reference from the business register</td>
    </tr>
    <tr>
      <td style="text-align:left">imputation_class</td>
      <td style="text-align:left">This variable is used to group observations for the purpose of imputation, e.g. calulating imputation links (forward link, backward link, construction link)</td>
    </tr>
    <tr>
      <td style="text-align:left">imputation_flags_adjustedresponse</td>
      <td style="text-align:left">An imputation marker that describes the source of the adjusted response to the given question. It takes the following values<br><br>fir :      Forwards imputation from response<br>fic :      Forwards imputation from construction<br>fimc :   Forwards imputation from manual construction<br>bir :      Backwards imputation<br>c :         Construction imputation from auxiliary variable<br>mc :     Manual construction<br>r :         Response</td>
    </tr>
    <tr>
      <td style="text-align:left">imputed_and_derived_flag</td>
      <td style="text-align:left">Takes the value 'd' when constrain_marker is sum[46, 47], sum[42, 23], or sum[40], to show that the response has been derived according to the ruleset defined by constrain_marker<br><br>Otherwise, it takes the value of imputation_flags_adjustedresponse</td>
    </tr>
    <tr>
      <td style="text-align:left">is_census</td>
      <td style="text-align:left">A  variable that states if the sampled business comes from a fully sampled strata, or census strata. It is given as a boolean.</td>
    </tr>
    <tr>
      <td style="text-align:left">is_sampled</td>
      <td style="text-align:left">A variable that states if the business was sampled in the final selection for a given period. It is used in calculating the estimation weights and given as a boolean, it will be True for all observations in the output.</td>
    </tr>
    <tr>
      <td style="text-align:left">l_value</td>
      <td style="text-align:left">A bias parameter used in the winsorisation process.<br>Data type: float<br>Source: The "l_values" are loaded from the file specified by "l_values_path". During the data-loading step, the column "l_value_input" is renamed to "l_value".<br>Usage in code: Defined as "l_values" within the "winsorise" function. Referenced as "l_value" in the "test_winsorisation" and "test_repalce_l_values" functions.</td>
    </tr>
    <tr>
      <td style="text-align:left">live_adjustedresponse</td>
      <td style="text-align:left">In a frozen run of the results pipeline this is a copy of the adjustedresponse variable as it is passed over from the responses pipeline before any imputation takes place.  For forms with status "Check needed" this column will contain the value that failed validation in the responses pipeline. In a live run of the results pipeline this variable will not exist.</td>
    </tr>
    <tr>
      <td style="text-align:left">new_target_variable</td>
      <td style="text-align:left">A derived column created during the winsorised weight calculation to calculate the outlier weight. It ensures that extreme values in "target_variable" are replaced with winsorised value ("new_target") to reduce the effect of outliers when calculating "outlier_weight".<br><br>Logic: If "target_variable" is less than or equal to "ratio_estimation_threshold", "new_target_variable" is set to the value of "target_variable",<br>Otherwise, it is set to the value of "new_target".<br><br>Data type: float</td>
    </tr>
    <tr>
      <td style="text-align:left">ni_gb_cell_number</td>
      <td style="text-align:left">Cell number as provided by IDBR and NISRA. This variable shows the raw cell number for Northern Ireland and Great Bristain before it is converted to UK cell number by changing the first digit of the NI cells from 7 to 5.</td>
    </tr>
    <tr>
      <td style="text-align:left">nw_ag_flag</td>
      <td style="text-align:left">A boolean flag used to indicate whether a record is excluded from winsorisation. A True means the record cannot be winsorised while a False means the record can be winsorised.<br>Data type: bool</td>
    </tr>
    <tr>
      <td style="text-align:left">outlier_weight</td>
      <td style="text-align:left">This is the weight that comes out of the winsorisation process. Sometimes referred to as "o_weight", it is designed to reduce the effect of outliers on the estimation process. This weight is later used to compute "winsorised_value" as part of the bias-correction process of the pipeline.<br>Data type: float</td>
    </tr>
    <tr>
      <td style="text-align:left">period</td>
      <td style="text-align:left">The year and month when the business was sampled and observation recorded. It is given as a numeric type, using the format YYYYMM to represent year and month. E.g., 202203 is March, 2022. For data manipulation within the source code, it is converted to the Python date type.</td>
    </tr>
    <tr>
      <td style="text-align:left">post_winsorised</td>
      <td style="text-align:left">post_winsorised is a boolean flag indicating the constraints applied to derived weighted responses are contravened, and therefore required the outlier weight of the derived question to be adjusted so that the weighted responses conform to the constraints. A tolerance is used to test whether the contraints hold.<br>Logic: False if the absolute difference between the winsoried value and the weighted responses for the constraint s is less than 10 exp(-tolerance). True otherwise.<br>Meaning: True means that the outlier weight of the derived variable has been updated. False means no update was required.<br>Data type: bool</td>
    </tr>
    <tr>
      <td style="text-align:left">pre_constrained_adjustedresponse</td>
      <td style="text-align:left">A copy of the `adjustedresponse` column immediately before the constraint logic is applied to derived questions. It can serve as a reference showing the `adjustedresponse` values prior to the application of contraints.<br>Data type: same as the target variable, typically float or int.</td>
    </tr>
    <tr>
      <td style="text-align:left">pre_derived_adjustedresponse</td>
      <td style="text-align:left">This is a copy of the adjustedresponse variable before derived questions are derived. It serves as a reference for tracking changes to the target variable (which may include imputed values) during the pipeline. This variable is used for the purpose of auditing, debugging and quality assurance of the analysis.<br><br>Data type: Matches the data type of the `target`(typically float or int)</td>
    </tr>
    <tr>
      <td style="text-align:left">predicted_unit_value</td>
      <td style="text-align:left">A derived column used in winsorisation that represents the predicted value per unit based on weighted target and auxiliary variables, excluding non-winsorised records. It provides an adjusted per unit prediction used in winsorisation, while excluding records that should not be winsorised (e.g. businesses in census cells).<br>Data type: float</td>
    </tr>
    <tr>
      <td style="text-align:left">questioncode</td>
      <td style="text-align:left">This variable identifies the question corresponding to the value given in response for a given observation. It takes the following values:<br><br>40   :  Total turnover<br>42   :  Commission or fees<br>43   :  Sales on own account<br>46   :  Total amount receivable in respect of invoices raised during period, including progress permits and excluding VAT, grants, and donations<br>47   :  Value of grants, donations, legacy investment, incorporated income and general funding, including fundraising<br>49   :  Value of exports<br>90   :  Excise duty<br>110 :  Total value of potable water supplied to customers in megalitres</td>
    </tr>
    <tr>
      <td style="text-align:left">ratio_estimation_treshold</td>
      <td style="text-align:left">A derived variable used in the winsorisation process to determine whether a target value should be replaced with its winsorised value.  The purpose is to establish the cutoff threshold used to deceide if a record's target value should be replaced with a winsorised value, while excluding census and flagged records from the winsorisation process.<br>Data type: float</td>
    </tr>
    <tr>
      <td style="text-align:left">reference</td>
      <td style="text-align:left">A unique reference number used to identify a business on the business register. It is an 11-digit number given as an integer.</td>
    </tr>
    <tr>
      <td style="text-align:left">region</td>
      <td style="text-align:left">A two-letter code that describes which region in Britain the observation/business was sampled from. It is given as a string, and takes the following values (region given in parentheses): AA (North East), BA (North West), BB (North West), DC (Yorkshire and The Humber), ED (East Midlands), FE (West Midlands), GF (East of England), GG (East of England), HH (London), JG (South East), KJ (South West), Wales (WW), Scotland (XX)</td>
    </tr>
    <tr>
      <td style="text-align:left">response</td>
      <td style="text-align:left">The raw response to a question provided by the contributor. This variable comes from the SPP responses pipeline and is not updated by the results pipeline. Full details to be found in the SPP reponses documentation.</td>
    </tr>
    <tr>
      <td style="text-align:left">sic_5_digit</td>
      <td style="text-align:left">A 5-digit Standard Industrial Classification (SIC) code used for mapping each record to its corresponding classification. The mapping is obtained from the "classification_sic_mapping.csv" file, which contains: "classification" (The target classification value) and "sic_5_digit" (the corresponding 5-dgit SIC code).<br><br>Data type: string (or int, if SIC codes are treated numerically)</td>
    </tr>
    <tr>
      <td style="text-align:left">status</td>
      <td style="text-align:left">A variable provided by the Statistical Production Platform denoting the status of the form returned by a contributor. It takes the following values:<br><br>Check needed  :  Validation process has been run on contributor responses and validation rules have been triggered<br>Clear  :  Validation process has been run on contributor responses and no validation rules have been triggered<br>Clear - overridden  :  Validation process has been run on contributor responses and validation rules have been triggered. User has manually indicated that all validation warnings are acceptable.<br>Combined child (NIL2)  :  Combined return. The contributor values are part of another references response. No response data is available under the given contributor.<br>Ceased trading (NIL4)  :  Company is no longer trading. No response data is available.<br>Dormant (NIL5)  :  Company is unable to return any responses due to lack of activity. No response data is available.<br>No UK activity (NIL9)  :  Contributor is unable to provide valid data as they operate outside of the UK. No response data is available.<br><br></td>
    </tr>
    <tr>
      <td style="text-align:left">statusencoded</td>
      <td style="text-align:left">The encoded variable provided by the Statistical Production Platform denoting the status of the form returned by a contributor. See status for the complete description. It takes the following values:<br><br>201  :  Clear<br>210  :  Check needed<br>211  :  Clear - overridden<br>302  :  Combined child (NIL2)<br>304  :  Ceased trading (NIL4)<br>305  :  Dormant (NIL5)<br>309  :  No UK activity (NIL9)</td>
    </tr>
    <tr>
      <td style="text-align:left">winsorised_value</td>
      <td style="text-align:left">A derived variable representing the adjusted winsorised value of the target variable after applying the outlier weight. The purpose is to produce the winsorised version of the target variable by scaling it using the calculated outlier weight. This adjusted value is then used in downstream processes.<br>Data type: float</td>
    </tr>
  </tbody>
</table>
