with score as (
with elx as (
with icd as
(
  select 
        hadm_id
      , seq_num  
      , CASE WHEN icd_version = 9 THEN icd_code ELSE NULL END AS icd9_code
      , CASE WHEN icd_version = 10 THEN icd_code ELSE NULL END AS icd10_code
  from `physionet-data.mimiciv_hosp.diagnoses_icd`
  where seq_num != 1 -- we do not include the primary code
)
, eliflg as
(
select hadm_id, seq_num, icd9_code, icd10_code
, CASE
  when SUBSTR(icd9_code, 1, 5) in ('39891','40201','40211','40291','40401','40403','40411','40413','40491','40493') then 1
  when SUBSTR(icd9_code, 1, 4) in ('4254','4255', '4256', '4257','4258','4259') then 1
  when SUBSTR(icd9_code, 1, 3) in ('428') then 1
  when SUBSTR(icd10_code, 1, 4) in ('I099','I110','I130','I132','I255','I420','I425','I426','I427','I428','I429','P290') then 1 
  when SUBSTR(icd10_code, 1, 3) in ('I43','I50') then 1
  else 0 end as CHF       /* Congestive heart failure */ 

, CASE
  when SUBSTR(icd9_code, 1, 5) in ('42613','42610','42612','99601','99604') then 1
  when SUBSTR(icd9_code, 1, 4) in ('4260','4267','4269','4270','4271','4272','4273','4274','4276','4277','4278','4279','7850','V450','V533') then 1
  when SUBSTR(icd10_code, 1, 4) in ('I441','I442','I443','I456','I459','R000','R001','R008','T821','Z450','Z950') then 1
  when SUBSTR(icd10_code, 1, 3) in ('I47', 'I49') then 1
  else 0 end as ARRHY

, CASE
  when SUBSTR(icd9_code, 1, 4) in ('0932','7463','7464','7465','7466','V422','V433') then 1
  when SUBSTR(icd9_code, 1, 3) in ('394','395','396','397','424') then 1
  when SUBSTR(icd10_code, 1, 4) in ('A520','I098','I091','Q230','Q231','Q232','Q233','Z952','Z953','Z954') then 1
  when SUBSTR(icd10_code, 1, 3) in ('I05','I06','I07', 'I08','I34','I35','I36','I37','I38','I39') then 1
  else 0 end as VALVE     /* Valvular disease */

, CASE
  when SUBSTR(icd9_code, 1, 4) in ('4150','4151','4170','4178','4179') then 1
  when SUBSTR(icd9_code, 1, 3) in ('416') then 1
  when SUBSTR(icd10_code, 1, 4) in ('I280','I288','I289') then 1
  when SUBSTR(icd10_code, 1, 3) in ('126','127') then 1
  else 0 end as PULMCIRC  /* Pulmonary circulation disorder */

, CASE
  when SUBSTR(icd9_code, 1, 4) in ('0930','4373','4431','4432','4433','4434','4435','4436','4437','4438','4439','4471','5571','5579','V434') then 1
  when SUBSTR(icd9_code, 1, 3) in ('440','441') then 1
  when SUBSTR(icd10_code, 1, 4) in ('I731','I738','I739','I771','I790','I792','K551','K558','K559','Z958','Z959') then 1
  when SUBSTR(icd10_code, 1, 3) in ('I70','171') then 1
  else 0 end as PERIVASC  /* Peripheral vascular disorder */

, CASE
  when SUBSTR(icd9_code, 1, 3) in ('401') then 1
  when SUBSTR(icd10_code, 1, 3) in ('I10') then 1
  else 0 end as HTN       /* Hypertension, uncomplicated */

, CASE
  when SUBSTR(icd9_code, 1, 3) in ('402','403','404','405') then 1
  when SUBSTR(icd10_code, 1, 3) in ('I11','I12','I13','I15') then 1
  else 0 end as HTNCX     /* Hypertension, complicated */

, CASE
  when SUBSTR(icd9_code, 1, 4) in ('3341','3440','3441','3442','3443','3444','3445','3446','3449') then 1
  when SUBSTR(icd9_code, 1, 3) in ('342','343') then 1
  when SUBSTR(icd10_code, 1, 4) in ('G041','G114','G801','G802','G830','G831','G832','G833','G834','G839') then 1
  when SUBSTR(icd10_code, 1, 3) in ('G81','G82') then 1
  else 0 end as PARA      /* Paralysis */

, CASE
  when SUBSTR(icd9_code, 1, 5) in ('33392') then 1
  when SUBSTR(icd9_code, 1, 4) in ('3319','3320','3321','3334','3335','3362','3481','3483','7803','7843') then 1
  when SUBSTR(icd9_code, 1, 3) in ('334','335','340','341','345') then 1
  when SUBSTR(icd10_code, 1, 4) in ('G254','G255','G312','G318','G319','G931','G934','R470') then 1
  when SUBSTR(icd10_code, 1, 3) in ('G10','G11','G12','G13','G20','G21','G22','G32','G35','G36','G37','G40','G41','R56') then 1
  else 0 end as NEURO     /* Other neurological */

, CASE
  when SUBSTR(icd9_code, 1, 4) in ('4168','4169','5064','5081','5088') then 1
  when SUBSTR(icd9_code, 1, 3) in ('490','491','492','493','494','495','496','497','498','499','500','501','502','503','504','505') then 1
  when SUBSTR(icd10_code, 1, 4) in ('I278','I279','J684','J701','J703') then 1 
  when SUBSTR(icd10_code, 1, 3) in ('J40','J41','J42','J43','J44','J45','J46','J47','J60','J61','J62','J63','J64','J65','J66','J67') then 1
  else 0 end as CHRNLUNG  /* Chronic pulmonary disease */

, CASE
  when SUBSTR(icd9_code, 1, 4) in ('2500','2501','2502','2503') then 1
  when SUBSTR(icd10_code, 1, 4) in ('E100','E101','E109','E110','E111','E119','E120','E121','E12.9','E130','E131','E139','E140','E141','E149') then 1
  else 0 end as DM        /* Diabetes w/o chronic complications*/

, CASE
  when SUBSTR(icd9_code, 1, 4) in ('2504','2505','2506','2507','2508','2509') then 1
  when SUBSTR(icd10_code, 1, 4) in ('E102','E103','E104','E105','E106','E107','E108',
                                    'E112','E113','E114','E115','E116','E117','E118',
                                    'E122','E123','E124','E125','E126','E127','E128',
                                    'E132','E133','E134','E135','E136','E137','E138',
                                    'E142','E143','E144','E145','E146','E147','E148') then 1
  else 0 end as DMCX      /* Diabetes w/ chronic complications */

, CASE
  when SUBSTR(icd9_code, 1, 4) in ('2409','2461','2468') then 1
  when SUBSTR(icd9_code, 1, 3) in ('243','244') then 1
  when SUBSTR(icd10_code, 1, 4) in ('E890') then 1
  when SUBSTR(icd10_code, 1, 3) in ('E00','E01','E02','E03') then 1
  else 0 end as HYPOTHY   /* Hypothyroidism */ 

, CASE
  when SUBSTR(icd9_code, 1, 5) in ('40301','40311','40391','40402','40403','40412','40413','40492','40493') then 1
  when SUBSTR(icd9_code, 1, 4) in ('5880','V420','V451') then 1
  when SUBSTR(icd9_code, 1, 3) in ('585','586','V56') then 1
  when SUBSTR(icd10_code, 1, 4) in ('I120','I131','N250','Z490','Z491','Z492','Z940','Z992') then 1
  when SUBSTR(icd10_code, 1, 3) in ('N18','N19') then 1
  else 0 end as RENLFAIL  /* Renal failure */

, CASE
  when SUBSTR(icd9_code, 1, 5) in ('07022','07023','07032','07033','07044','07054') then 1
  when SUBSTR(icd9_code, 1, 4) in ('0706','0709','4560','4561','4562','5722','5723','5724','5725','5726','5727','5728','5733','5734','5738','5739','V427') then 1
  when SUBSTR(icd9_code, 1, 3) in ('570','571') then 1
  when SUBSTR(icd10_code, 1, 4) in ('I864','I982','K711','K713','K714','K715','K717','K760','K762','K763','K764','K765','K766','K767','K768','K769','Z944') then 1
  when SUBSTR(icd10_code, 1, 3) in ('B18','I85','K70','K72','K73','K74') then 1
  else 0 end as LIVER     /* Liver disease */

, CASE
  when SUBSTR(icd9_code, 1, 4) in ('5317','5319','5327','5329','5337','5339','5347','5349') then 1
  when SUBSTR(icd10_code, 1, 4) in ('K257','K259','K267','K269','K277','K279','K287','K289') then 1 
  else 0 end as ULCER     /* Chronic Peptic ulcer disease (includes bleeding only if obstruction is also present) */

, CASE
  when SUBSTR(icd9_code, 1, 3) in ('042','043','044') then 1
  when SUBSTR(icd10_code, 1, 3) in ('B20','B21','B22','B24') then 1
  else 0 end as AIDS      /* HIV and AIDS */

, CASE
  when SUBSTR(icd9_code, 1, 4) in ('2030','2386') then 1
  when SUBSTR(icd9_code, 1, 3) in ('200','201','202') then 1
  when SUBSTR(icd10_code, 1, 4) in ('C900','C902') then 1 
  when SUBSTR(icd10_code, 1, 3) in ('C81','C82','C83','C84','C85','C85','C88','C96') then 1
  else 0 end as LYMPH     /* Lymphoma */

, CASE
  when SUBSTR(icd9_code, 1, 3) in ('196','197','198','199') then 1
  when SUBSTR(icd10_code, 1, 3) in ('C77','C78','C79','C80') then 1
  else 0 end as METS      /* Metastatic cancer */

, CASE
  when SUBSTR(icd9_code, 1, 3) in
  (
     '140','141','142','143','144','145','146','147','148','149','150','151','152'
    ,'153','154','155','156','157','158','159','160','161','162','163','164','165'
    ,'166','167','168','169','170','171','172','174','175','176','177','178','179'
    ,'180','181','182','183','184','185','186','187','188','189','190','191','192'
    ,'193','194','195'
  ) then 1
  when SUBSTR(icd10_code, 1, 3) in 
  (
      'C00','C01','C02','C03','C04','C05','C06','C07','C08','C09','C10','C11','C12', 
      'C13','C14','C15','C16','C17','C18','C19','C20','C21','C22','C23','C24','C25',
      'C26','C30','C31','C32','C33','C34','C37','C38','C39','C40','C41','C43','C45', 
      'C46','C47','C48','C49','C50','C51','C52','C53','C54','C55','C56','C57','C58',
      'C60','C61','C62','C63','C64','C65','C66','C67','C68','C69','C70','C71','C72',
      'C73','C74','C75','C76','C97'
  ) then 1
  else 0 end as TUMOR     /* Solid tumor without metastasis */

, CASE
  when SUBSTR(icd9_code, 1, 5) in ('72889','72930') then 1
  when SUBSTR(icd9_code, 1, 4) in ('7010','7100','7101','7102','7103','7104','7108','7109','7112','7193','7285') then 1
  when SUBSTR(icd9_code, 1, 3) in ('446','714','720','725') then 1
  when SUBSTR(icd10_code, 1, 4) in ('L940','L941','L943','M120','M123','M310','M311','M312','M313','M461','M468','M469') then 1
  when SUBSTR(icd10_code, 1, 3) in ('M05','M06','M08','M30','M32','M33','M34','M35','M45') then 1
  else 0 end as ARTH              /* Rheumatoid arthritis/collagen vascular diseases */

, CASE
  when SUBSTR(icd9_code, 1, 4) in ('2871','2873','2874','2875') then 1
  when SUBSTR(icd9_code, 1, 3) in ('286') then 1
  when SUBSTR(icd10_code, 1, 4) in ('D691','D693','D694','D695','D696') then 1 
  when SUBSTR(icd10_code, 1, 3) in ('D65','D66','D67','D68') then 1
  else 0 end as COAG      /* Coagulation deficiency */

, CASE
  when SUBSTR(icd9_code, 1, 4) in ('2780') then 1
  when SUBSTR(icd10_code, 1, 3) in ('E66') then 1
  else 0 end as OBESE     /* Obesity      */

, CASE
  when SUBSTR(icd9_code, 1, 4) in ('7832','7994') then 1
  when SUBSTR(icd9_code, 1, 3) in ('260','261','262','263') then 1
  when SUBSTR(icd10_code, 1, 4) in ('R634') then 1
  when SUBSTR(icd10_code, 1, 3) in ('E40','E41','E42','E43','E44','E45','E46','R64') then 1
  else 0 end as WGHTLOSS  /* Weight loss */ 

, CASE
  when SUBSTR(icd9_code, 1, 4) in ('2536') then 1
  when SUBSTR(icd9_code, 1, 3) in ('276') then 1
  when SUBSTR(icd10_code, 1, 4) in ('E222') then 1 
  when SUBSTR(icd10_code, 1, 3) in ('E86','E87') then 1
  else 0 end as LYTES     /* Fluid and electrolyte disorders */ 

, CASE
  when SUBSTR(icd9_code, 1, 4) in ('2800') then 1
  when SUBSTR(icd10_code, 1, 4) in ('D500') then 1
  else 0 end as BLDLOSS   /* Blood loss anemia */

, CASE 
  when SUBSTR(icd9_code, 1, 4) in ('2801','2802','2803','2804','2805','2806','2807','2808','2809') then 1
  when SUBSTR(icd9_code, 1, 3) in ('281') then 1
  when SUBSTR(icd10_code, 1, 3) in ('D508','D509') then 1
  when SUBSTR(icd10_code, 1, 3) in ('D51','D52','D53') then 1
  else 0 end as ANEMDEF  /* Deficiency anemias */

, CASE
  when SUBSTR(icd9_code, 1, 4) in 
  (
      '2652','2911','2912','2913','2915','2916','2917','2918','2919','3030','3039',
      '3050','3575','4255','5353','5710','5711','5712','5713','V113'
  ) then 1
  when SUBSTR(icd9_code, 1, 3) in ('980') then 1
  when SUBSTR(icd10_code, 1, 4) in ('G621','I426','K292','K700','K703','K709','Z502','Z714','Z721') then 1
  when SUBSTR(icd10_code, 1, 3) in ('F10','E52','T51') then 1
  else 0 end as ALCOHOL /* Alcohol abuse */

, CASE
  when SUBSTR(icd9_code, 1, 5) in ('V6542') then 1
  when SUBSTR(icd9_code, 1, 4) in ('3052','3053','3054','3055','3056','3057','3058','3059') then 1
  when SUBSTR(icd9_code, 1, 3) in ('292','304') then 1
  when SUBSTR(icd10_code, 1, 4) in ('Z715','Z722') then 1
  when SUBSTR(icd10_code, 1, 3) in ('F11','F12','F13','F14','F15','F16','F18','F19') then 1
  else 0 end as DRUG /* Drug abuse */

, CASE
  when SUBSTR(icd9_code, 1, 5) in ('29604','29614','29644','29654') then 1
  when SUBSTR(icd9_code, 1, 4) in ('2938') then 1
  when SUBSTR(icd9_code, 1, 3) in ('295','297','298') then 1
  when SUBSTR(icd10_code, 1, 4) in ('F302','F312','F315') then 1 
  when SUBSTR(icd10_code, 1, 3) in ('F20','F22','F23','F24','F25','F28','F29') then 1
  else 0 end as PSYCH /* Psychoses */

, CASE
  when SUBSTR(icd9_code, 1, 4) in ('2962','2963','2965','3004') then 1
  when SUBSTR(icd9_code, 1, 3) in ('309','311') then 1
  when SUBSTR(icd10_code, 1, 4) in ('F204','F313','F314','F315','F341','F412','F432') then 1 
  when SUBSTR(icd10_code, 1, 3) in ('F32','F33') then 1
  else 0 end as DEPRESS  /* Depression */
from icd
)
-- collapse the ICD specific flags into hadm_id specific flags
-- this groups comorbidities together for a single patient admission
, eligrp as
(
  select hadm_id
  , max(chf) as chf
  , max(arrhy) as arrhy
  , max(valve) as valve
  , max(pulmcirc) as pulmcirc
  , max(perivasc) as perivasc
  , max(htn) as htn
  , max(htncx) as htncx
  , max(para) as para
  , max(neuro) as neuro
  , max(chrnlung) as chrnlung
  , max(dm) as dm
  , max(dmcx) as dmcx
  , max(hypothy) as hypothy
  , max(renlfail) as renlfail
  , max(liver) as liver
  , max(ulcer) as ulcer
  , max(aids) as aids
  , max(lymph) as lymph
  , max(mets) as mets
  , max(tumor) as tumor
  , max(arth) as arth
  , max(coag) as coag
  , max(obese) as obese
  , max(wghtloss) as wghtloss
  , max(lytes) as lytes
  , max(bldloss) as bldloss
  , max(anemdef) as anemdef
  , max(alcohol) as alcohol
  , max(drug) as drug
  , max(psych) as psych
  , max(depress) as depress
from eliflg
group by hadm_id
)
-- now merge these flags together to define elixhauser
-- most are straightforward.. but hypertension flags are a bit more complicated

select adm.hadm_id
, chf as CONGESTIVE_HEART_FAILURE
, arrhy as CARDIAC_ARRHYTHMIAS
, valve as VALVULAR_DISEASE
, pulmcirc as PULMONARY_CIRCULATION
, perivasc as PERIPHERAL_VASCULAR
-- we combine "htn" and "htncx" into "HYPERTENSION"
, case
    when htn = 1 then 1
    when htncx = 1 then 1
  else 0 end as HYPERTENSION
, para as PARALYSIS
, neuro as OTHER_NEUROLOGICAL
, chrnlung as CHRONIC_PULMONARY
-- only the more severe comorbidity (complicated diabetes) is kept
, case
    when dmcx = 1 then 0
    when dm = 1 then 1
  else 0 end as DIABETES_UNCOMPLICATED
, dmcx as DIABETES_COMPLICATED
, hypothy as HYPOTHYROIDISM
, renlfail as RENAL_FAILURE
, liver as LIVER_DISEASE
, ulcer as PEPTIC_ULCER
, aids as AIDS
, lymph as LYMPHOMA
, mets as METASTATIC_CANCER
-- only the more severe comorbidity (metastatic cancer) is kept
, case
    when mets = 1 then 0
    when tumor = 1 then 1
  else 0 end as SOLID_TUMOR
, arth as RHEUMATOID_ARTHRITIS
, coag as COAGULOPATHY
, obese as OBESITY
, wghtloss as WEIGHT_LOSS
, lytes as FLUID_ELECTROLYTE
, bldloss as BLOOD_LOSS_ANEMIA
, anemdef as DEFICIENCY_ANEMIAS
, alcohol as ALCOHOL_ABUSE
, drug as DRUG_ABUSE
, psych as PSYCHOSES
, depress as DEPRESSION

FROM `physionet-data.mimiciv_hosp.admissions` adm
left join eligrp eli
  on adm.hadm_id = eli.hadm_id
order by adm.hadm_id)

select  elx.*
,  -- Below is the van Walraven score
   0 * AIDS +
   0 * ALCOHOL_ABUSE +
  -2 * BLOOD_LOSS_ANEMIA +
   7 * CONGESTIVE_HEART_FAILURE +
   -- Cardiac arrhythmias are not included in van Walraven based on Quan 2007
   3 * CHRONIC_PULMONARY +
   3 * COAGULOPATHY +
  -2 * DEFICIENCY_ANEMIAS +
  -3 * DEPRESSION +
   0 * DIABETES_COMPLICATED +
   0 * DIABETES_UNCOMPLICATED +
  -7 * DRUG_ABUSE +
   5 * FLUID_ELECTROLYTE +
   0 * HYPERTENSION +
   0 * HYPOTHYROIDISM +
   11 * LIVER_DISEASE +
   9 * LYMPHOMA +
   12 * METASTATIC_CANCER +
   6 * OTHER_NEUROLOGICAL +
  -4 * OBESITY +
   7 * PARALYSIS +
   2 * PERIPHERAL_VASCULAR +
   0 * PEPTIC_ULCER +
   0 * PSYCHOSES +
   4 * PULMONARY_CIRCULATION +
   0 * RHEUMATOID_ARTHRITIS +
   5 * RENAL_FAILURE +
   4 * SOLID_TUMOR +
  -1 * VALVULAR_DISEASE +
   6 * WEIGHT_LOSS
as elixhauser_vanwalraven



,  -- Below is the 29 component SID score
   0 * AIDS +
  -2 * ALCOHOL_ABUSE +
  -2 * BLOOD_LOSS_ANEMIA +
   -- Cardiac arrhythmias are not included in SID-29
   9 * CONGESTIVE_HEART_FAILURE +
   3 * CHRONIC_PULMONARY +
   9 * COAGULOPATHY +
   0 * DEFICIENCY_ANEMIAS +
  -4 * DEPRESSION +
   0 * DIABETES_COMPLICATED +
  -1 * DIABETES_UNCOMPLICATED +
  -8 * DRUG_ABUSE +
   9 * FLUID_ELECTROLYTE +
  -1 * HYPERTENSION +
   0 * HYPOTHYROIDISM +
   5 * LIVER_DISEASE +
   6 * LYMPHOMA +
   13 * METASTATIC_CANCER +
   4 * OTHER_NEUROLOGICAL +
  -4 * OBESITY +
   3 * PARALYSIS +
   0 * PEPTIC_ULCER +
   4 * PERIPHERAL_VASCULAR +
  -4 * PSYCHOSES +
   5 * PULMONARY_CIRCULATION +
   6 * RENAL_FAILURE +
   0 * RHEUMATOID_ARTHRITIS +
   8 * SOLID_TUMOR +
   0 * VALVULAR_DISEASE +
   8 * WEIGHT_LOSS
as elixhauser_SID29


,  -- Below is the 30 component SID score
   0 * AIDS +
   0 * ALCOHOL_ABUSE +
  -3 * BLOOD_LOSS_ANEMIA +
   8 * CARDIAC_ARRHYTHMIAS +
   9 * CONGESTIVE_HEART_FAILURE +
   3 * CHRONIC_PULMONARY +
  12 * COAGULOPATHY +
   0 * DEFICIENCY_ANEMIAS +
  -5 * DEPRESSION +
   1 * DIABETES_COMPLICATED +
   0 * DIABETES_UNCOMPLICATED +
 -11 * DRUG_ABUSE +
  11 * FLUID_ELECTROLYTE +
  -2 * HYPERTENSION +
   0 * HYPOTHYROIDISM +
   7 * LIVER_DISEASE +
   8 * LYMPHOMA +
  17 * METASTATIC_CANCER +
   5 * OTHER_NEUROLOGICAL +
  -5 * OBESITY +
   4 * PARALYSIS +
   0 * PEPTIC_ULCER +
   4 * PERIPHERAL_VASCULAR +
  -6 * PSYCHOSES +
   5 * PULMONARY_CIRCULATION +
   7 * RENAL_FAILURE +
   0 * RHEUMATOID_ARTHRITIS +
  10 * SOLID_TUMOR +
   0 * VALVULAR_DISEASE +
  10 * WEIGHT_LOSS
as elixhauser_SID30
from  elx)
select i.stay_id, e.*EXCEPT(hadm_id) from --ALTERED for column selection
`physionet-data.mimiciv_icu.icustays` as i
left join 
score as e
on i.hadm_id = e.hadm_id
