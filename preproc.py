import sys
import pandas as pd


def concat_messages(
        all_messages_file: str,
        tmp_file: str
        ) -> None:
    '''Concatenate new and old messages and rewrite to [all_messages_file]'''

    df = pd.read_csv(
        tmp_file, sep='\t',
        encoding='utf-8',
        parse_dates=['datetime'])

    try:
        fd = pd.read_csv(
            all_messages_file,
            sep='\t',
            encoding='utf-8',
            parse_dates=['datetime'])
        
        df = pd.concat((
            fd, df), axis=0).drop_duplicates()
        
        df.sort_values(
            by=['datetime'],
            ascending=True,
            inplace=True,
            ignore_index=True)
        
    except:
        print('Create new all_messages_file')

    finally:
        df.to_csv(
            all_messages_file,
            sep='\t',
            index=False,
            encoding='utf-8')


def messages_df_to_timeline(all_messages_file: str) -> None:
    '''Transform messages dataframe to timeline and save it'''

    df = pd.read_csv(
        all_messages_file,
        sep='\t',
        encoding='utf-8',
        parse_dates=['datetime']
        )

    df = df.groupby([pd.Grouper(
        key='datetime',
        freq='H')]).agg(txt_cnt=('text',
                                 'count')).reset_index()

    new_filename = all_messages_file[:-4] + '_cnt' + all_messages_file[-4:]
    
    df.to_csv(
        new_filename,
        sep='\t',
        index=False,
        encoding='utf-8')


if __name__ == "__main__":

    try:
        all_messages_file = sys.argv[1]
        tmp_file = sys.argv[2]   

    except:
        raise ValueError ('''Please use correct parameters:
            [1]-all mesages file name,
            [2]-tmp file with new messages''')
    
    concat_messages(all_messages_file, tmp_file)
    
    messages_df_to_timeline(all_messages_file)