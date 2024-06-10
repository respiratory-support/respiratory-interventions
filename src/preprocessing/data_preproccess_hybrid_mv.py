import pandas as pd
import numpy as np
import os

from sklearn.preprocessing import MinMaxScaler


def load_cache():
  MAX_CASES = 50920 
  CACHE_FILE_NAME= f'mv_{MAX_CASES}_cases.npz'

  file_path  = os.path.join('/ML4HC', CACHE_FILE_NAME)
  if os.path.exists(CACHE_FILE_NAME):
    print('reading cache file', end='...', flush=True)
    data=np.load(CACHE_FILE_NAME, allow_pickle = True)
    y=data['y']
    print(f'{len(y)} cases', flush=True)
    x = pd.DataFrame(data['x'], columns=data['column_names'])
    print('loaded data.....')
    return x, y
  
  else:
    path = '/ML4HC' 
    file_list = os.listdir(path)
    frames = []
    label_y=[]
    for file_name in file_list[:MAX_CASES]:  
      df = pd.read_csv(path + '/' + file_name, index_col=0)
      if any(df['death_outcome'] == 1):
          continue
      df.ffill(axis=0, inplace=True)
      
      y = df['invasive'].to_numpy()
      start_idx = np.where(y == 1)[0]
      if len(start_idx) == 0:
          continue
      else:
        start_idx = start_idx[0]
        end_idx = np.where(y[start_idx:] == 0)[0]
        if len(end_idx) == 0:
            continue
        else:
            y = end_idx[0]
      if y <= 24:
          continue
      df = df[(df['hr'] >= start_idx) & (df['hr'] < start_idx + 24)]
      if len(df) < 24 :
        continue
      assert (df['death_outcome'].all() == 0)
      frames.append(df)
      label_y.append(y>14*24) 
    
    df = pd.concat(frames)
    print(df)
    df.fillna(value = -1, inplace = True)
    df.reset_index(inplace=True)
    
    mask_anchor_year = ~(result['anchor_year'] == -1)
    result ['new_age'] = np.nan 

    result.loc[mask_anchor_year, 'new_age'] = result.loc[mask_anchor_year,'anchor_age']+ \
                                            (pd.to_datetime(result.loc[mask_anchor_year,'anchor_year'], format = '%Y') - \
                                            pd.to_datetime(result.loc[mask_anchor_year,'intime'])).dt.days/365.2425
    print('saving cache file', end='...', flush=True)
    
    np.savez_compressed(CACHE_FILE_NAME, x=result.to_numpy(), y=np.array(label_y), column_names=np.array(result.columns))
    return result, np.array(label_y)


def load_data():

    df,y= load_cache()

    print(df.shape, ".............................")
  
    df_stat = df[['elixhauser_vanwalraven','gender',
                  'first_careunit','new_age','gcs', 'gcs_motor', 'gcs_verbal', 'gcs_eyes', 'gcs_unable',
                  'pbw_kg']]
    df_stat['gender'] = df_stat['gender'] == 'F'
    df_stat=pd.get_dummies(df_stat, 
                           columns = ['gender',
                                      'first_careunit'], drop_first=True)
    df_stat=df_stat.astype(float) 
    scaler = MinMaxScaler(feature_range=(0, 1))
    stat = scaler.fit_transform(df_stat)
    x_stat = stat.reshape(-1, 24, len(df_stat.columns))
    x_stat = x_stat[:, 0, :]   
    
   
    df_cont = df[['ppeak', 'set_peep',
        'total_peep', 'rr', 'set_rr', 'total_rr', 'set_tv',
        'total_tv', 'fio2', 'set_ie_ratio', 'set_pc',
        'calculated_bicarbonate', 'pCO2', 'pH','pO2', 'so2',
        'vasopressor', 'crrt', 'heart_rate', 'sbp', 'dbp', 'mbp', 'sbp_ni',
        'dbp_ni', 'mbp_ni', 'temperature', 'spo2', 'glucose', 'sepsis3',
        'sofa_24hours']]
    
    cont = scaler.fit_transform(df_cont)
    x_cont = cont.astype(float).reshape(-1, 24, len(df_cont.columns)) 
    
    
    stay_ids = np.unique(df['stay_id'])
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
    
    train_mask = np.isin(stay_ids, train_caseids)
    test_mask = np.isin(stay_ids, test_caseids)
    x_train = [x_cont[train_mask], x_stat[train_mask]] 
    y_train = y[train_mask]
    x_test = [x_cont[test_mask],x_stat[test_mask]]
    y_test = y[test_mask]

    test_ids =pd.DataFrame(test_caseids, columns=['CaseID'])
    test_ids.to_csv('testids_hybird_mv.csv')
    
    print('train: {} cases {} samples, testing: {} cases {} samples'.format(len(train_caseids), np.sum(train_mask), len(test_caseids), np.sum(test_mask)))

    return  x_train, y_train, x_test, y_test
