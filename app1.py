from asyncore import write
from distutils.log import error
from pickle import TRUE
from platform import processor
from time import sleep
import zipfile
import streamlit as st
from zipfile import ZipFile
from urllib.request import urlretrieve
import pandas as pd
import os
import validators
import pandas as pd
import numpy as np
import shutil
from data_processor import DataProcessor
from utils import join_results

# Initialize
if 'validUrl' not in st.session_state:
    st.session_state['validUrl'] = False
if 'loadingText' not in st.session_state:
    st.session_state['loadingText'] = ''
if "processor" not in st.session_state:
    st.session_state['processor'] = DataProcessor()
if "file_downloaded" not in st.session_state: 
    st.session_state['file_downloaded'] = False

# functions


def removeError():
    if 'zipError' in st.session_state:
        del st.session_state['zipError']

def retrieve_and_process_internel():
    data_processor = st.session_state['processor']
    data_processor.set_url(st.session_state.urlInput)

    successfully_download = data_processor.download_and_process_file()

    if not successfully_download:
        st.session_state['zipError'] = 'The url you enter neither valid or contians a zipFile'
        st.session_state['loadingText'] = ''
        return
    removeError()
    st.session_state['loadingText'] = 'Finish processing'
    st.session_state['file_downloaded'] = True

def retrieve_and_process():
    st.session_state['loadingText'] = 'processing data......'
    retrieve_and_process_internel();


def start_face_recognition():
    data_processor = st.session_state['processor']
    data_processor.execute()
    df = data_processor.convert_perosn_to_df()
    output = join_results(df)
    st.session_state['face_result'] = output
    



# UI
'''[Visit my github](https://github.com/wuyiming0308/streamlit-app)'''
st.write('Step 1: Procide Milestone 2 Input File')


def validateUrl():
    removeError()
    st.session_state['validUrl'] = validators.url(st.session_state.urlInput)
    st.session_state['"file_downloaded"'] = False
    st.session_state['loadingText'] = ''


def appendExampleUrl():
    st.session_state.urlInput = 'https://www.dropbox.com/s/tyjlh1tjo0rh7e4/5109666.zip?dl=1'
    validateUrl()


st.button(label='try example url', on_click=appendExampleUrl)

formc1, formc2 = st.columns([3, 1])
with formc1:
    st.text_input(label='Url Input', value='', key='urlInput',
                  on_change=validateUrl, placeholder='Enter URL of Test Data')


with formc2:
    st.write('')
    st.write('')
    st.button(label='Try download file', disabled=not st.session_state.validUrl,
              on_click=retrieve_and_process)

if st.session_state.validUrl:
    st.write('The url you enter is', st.session_state.urlInput, '.')
elif st.session_state.urlInput != '':
    st.error('Please enter a valid url!!')

# present loading state
if  st.session_state['loadingText'] != '':
    loadingstate = st.session_state['loadingText']
    st.markdown(f'<p style="color:green"> {loadingstate}.......</p>', unsafe_allow_html=True)

if 'zipError' in st.session_state:
    st.error(st.session_state.zipError)

# face recogantion
st.write("Step 2: Start processing")
st.button(label='start face recognition', disabled=not st.session_state.file_downloaded,
              on_click=start_face_recognition)


st.write('Step 3: Preview and Download Data')


@st.cache
def convert_df_to_csv(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv(index=False).encode('utf-8')


if 'face_result' in st.session_state:
    st.download_button(
        label='Download',
        data=convert_df_to_csv(st.session_state.face_result),
        file_name=st.session_state.urlInput[-16:-9]+'.csv',
        mime='text/csv',
    )
    st.write(st.session_state['face_result'])
else:
    st.button(
        label='No data',
        disabled=True
    )

   
    
