import pandas as pd

#이 코드는 구글 코렙을 기준으로 작성한 코드입니다

# 파일 경로 변수
library_file = '도서관 파일 이름'
book_file = '공공 도서관 파일 이름'

# 도서관 데이터 로드
library = pd.read_csv(library_file, encoding='utf8', engine='python')
# 책 데이터 로드
book = pd.read_csv(book_file, encoding='utf8', engine='python')

#도서관 데이터 전처리
required_columns = ['LBRRY_CD','LBRRY_NM','LBRRY_ADDR', 'ONE_AREA_NM','TEL_NO', 'HMPG_VALUE']

library_selected = library[required_columns]

library_selected = library_selected[library_selected['LBRRY_ADDR'].notnull()]
library_selected = library_selected[library_selected['TEL_NO'].notnull()]
library_selected = library_selected[library_selected['HMPG_VALUE'].notnull()]
library_selected['LBRRY_ADDR'] = library_selected['LBRRY_ADDR'].apply(lambda x: x.replace(',', ''))

#부산광역시의 도서관 정보만 가져오기
busan=library_selected[library_selected['ONE_AREA_NM']=='부산광역시']
busan_lbrary_cd = busan[['LBRRY_CD']]

#책 데이터 전처리
required_columns = ['AUTHR_NM', 'PBLICTE_YEAR','TITLE_NM', 'LBRRY_CD', 'REGIST_NO']

# 필요한 컬럼만 추출
book_selected = book[required_columns]

book_selected = book_selected[book_selected['AUTHR_NM'].notnull()]
book_selected = book_selected[book_selected['PBLICTE_YEAR'].notnull()]
book_selected = book_selected[book_selected['REGIST_NO'].notnull()]
book_selected['AUTHR_NM'] = book_selected['AUTHR_NM'].apply(lambda x: x.replace(',', ' '))
book_selected['TITLE_NM'] = book_selected['TITLE_NM'].apply(lambda x: x.replace(',', ''))

busan_list=busan_lbrary_cd['LBRRY_CD'].tolist()
#부산 지역의 책 데이터 추출
book_df = book_selected[book_selected['LBRRY_CD'].isin(busan_list)]

# 필요한 컬럼만 선택
library_df_selected = busan[['LBRRY_CD', 'LBRRY_NM']]
book_df_selected = book_df[['AUTHR_NM', 'PBLICTE_YEAR', 'TITLE_NM', 'LBRRY_CD', 'REGIST_NO']]

# 데이터 병합 (LBRRY_CD 기준)
merged_df = pd.merge(book_df_selected, library_df_selected, on='LBRRY_CD', how='left')

# 병합된 데이터프레임 저장
merged_df.to_csv('busan_books.csv', index=False, encoding='utf-8')

'''
#정제 완료 후 100개의 데이터 랜덤 추출 코드

# 상위 100개의 데이터를 랜덤으로 추출합니다.
random_100_df = merged_df.sample(n=100)

# 랜덤으로 추출한 100개의 데이터를 새로운 CSV 파일로 저장합니다.
random_100_df.to_csv('Busan_book_random_100.csv', index=False, encoding='utf-8')
'''