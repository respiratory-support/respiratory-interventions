import pandas as pd
import numpy as np
import os
from sklearn.preprocessing import MinMaxScaler


def load_cache(days):
  MAX_CASES = 50920
  CACHE_FILE_NAME= f'df_{MAX_CASES}_{days}.npz' 
  file_path  = os.path.join('ML4H_folder', CACHE_FILE_NAME)
  if os.path.exists(file_path):
    print('reading cache file', end='...', flush=True)
    data=np.load(file_path, allow_pickle = True)
    y=data['y']
    print(y, 'THIS IS LOADING CACHE y')
    print(f'{len(y)} cases', flush=True)
    x = pd.DataFrame(data['x'], columns=data['column_names'])
    print('loaded data.....')
    return x, y  
  
  else:
    path = '/subjects' 
    file_list = os.listdir(path)
    print(len(file_list)) 
    frames = []
    label_y=[]
    subjects_excluded = {'no mv' : 0,
                         'no weaning' : 0,
                         'weaning within the last 7 days of the stay' : 0,
                         'weaning within the first 4 days of the stay' : 0,
                          }
    for file_name in file_list[:MAX_CASES]:  
    
          df = pd.read_csv(path + '/' + file_name)
          df.loc[df['death_outcome'] == 1, 'invasive'] = 1 

          df.ffill(axis=0, inplace=True)
          y = df['invasive'].to_numpy()  
          df = df.drop(['icuouttime_outcome', 'discharge_outcome'], axis=1)

          start_idx = np.where(y == 1)[0] 
          if len(start_idx) == 0:
              subjects_excluded['no mv'] += 1 
              continue
          start_idx = start_idx[0] 
          length_of_mv = np.where(y[start_idx:] == 0)[0] 
          if len(length_of_mv) == 0: 
            subjects_excluded['no weaning'] += 1 
            continue 
          length_of_mv = length_of_mv[0]

          weaning_idx = start_idx + length_of_mv 
          length_of_weaning = np.where(y[weaning_idx:] == 0)[0] 
          if weaning_idx + days*24 > len(y): 
            subjects_excluded['weaning within the last 7 days of the stay'] += 1 
            continue 
          y = length_of_weaning[-1] + 1 

          df_x = df[(df['hr'] >= weaning_idx - 5*24) & (df['hr'] < weaning_idx)]

          if len(df_x) < 5*24 :  
            subjects_excluded['weaning within the first 4 days of the stay'] += 1  
            continue
          frames.append(df_x)   
          label_y.append(y)

    print(len(frames))
    df = pd.concat(frames) 

    df.fillna(value = -1, inplace = True)
    df.reset_index(inplace=True)
    result = df
    mask_anchor_year = ~(df['anchor_year'] == -1)
    result ['new_age'] = np.nan 
    result.loc[mask_anchor_year, 'new_age'] = result.loc[mask_anchor_year,'anchor_age']+ \
                                            (pd.to_datetime(result.loc[mask_anchor_year,'anchor_year'], format = '%Y') - \
                                            pd.to_datetime(result.loc[mask_anchor_year,'intime'])).dt.days/365.2425
    print('saving cache file', end='...', flush=True)
    
    file_path  = os.path.join('/ML4H_folder', CACHE_FILE_NAME)
    np.savez_compressed(file_path, x=result.to_numpy(), y=np.array(label_y), 
                        column_names=np.array(result.columns))
           
    return result, np.array(label_y)

def load_data(days=7):
     
    df,y = load_cache(days)  
    
    print(df.shape, ".............................")  
    df_stat = df[['elixhauser_vanwalraven','gender',
                  'first_careunit','new_age','gcs', 'gcs_motor', 'gcs_verbal', 'gcs_eyes', 'gcs_unable',
                  'pbw_kg']]
    df_stat['gender'] = df_stat['gender'] == 'F'
    df_stat=pd.get_dummies(df_stat, 
                           columns = ['gender',
                                      'first_careunit'])
    stay_ids = np.unique(df['stay_id'])
    hadm_ids = np.unique(df['hadm_id']) 
    
    df_stat=df_stat.astype(float) 
    scaler = MinMaxScaler(feature_range=(0, 1))
    stat = scaler.fit_transform(df_stat)
    
    x_stat = stat.reshape(-1, 5*24, len(df_stat.columns))
    
    x_stat = x_stat[:, 0, :] 
    cont_list = ['ppeak', 'set_peep',          
        'total_peep', 'rr', 'set_rr', 'total_rr', 'set_tv',
        'total_tv', 'set_fio2', 'set_ie_ratio', 'set_pc',
        'calculated_bicarbonate', 'pCO2', 'pH','pO2', 'so2',
        'vasopressor', 'crrt', 'heart_rate', 'sbp', 'dbp', 'mbp', 'sbp_ni',
        'dbp_ni', 'mbp_ni', 'temperature', 'spo2', 'glucose', 'sepsis3',
        'sofa_24hours']
    df_cont = df[cont_list]
        
    cont = scaler.fit_transform(df_cont)
    x_cont = cont.astype(float).reshape(-1, 5*24, len(df_cont.columns)) 
    y = y > days*24

    print(stay_ids.shape)
    print(y.shape) 
    print(x_cont.shape) 
    print(x_stat.shape) 
    ncase = len(stay_ids)  
    ntest = int(ncase * 0.2) 
    if ntest == 0: 
        ntest = 1
    ntrain = ncase - ntest
    train_caseids = stay_ids[:ntrain]
    test_caseids = stay_ids[ntrain:ncase]
    test_ids =pd.DataFrame(test_caseids, columns=['CaseID'])
    test_ids.to_csv('testids_hybrid_sw.csv')

    train_mask = np.isin(stay_ids, train_caseids)
    test_mask = np.isin(stay_ids, test_caseids)
  
    x_train = [x_cont[train_mask], x_stat[train_mask]]
    y_train = y[train_mask]
    x_test = [x_cont[test_mask],x_stat[test_mask]]
    y_test = y[test_mask]
 
    print(len(x_train[0].shape))
    print(len(x_train[1].shape))
    print(x_cont.shape)
    print(x_stat.shape)

    from collections import Counter
    print(f'train: {len(train_caseids)} cases {np.sum(train_caseids)} samples, mean of y_train {np.mean(y_train)}\
          testing: {len(test_caseids)} cases {np.sum(test_mask)} samples, mean of y_test {np.mean(y_test)}')

    return  x_train, y_train, x_test, y_test
 
if __name__ == "__main__":
  load_data()
