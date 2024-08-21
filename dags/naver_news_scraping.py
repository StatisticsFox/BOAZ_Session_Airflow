from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag, task
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# WebDriver 초기화 함수
def init_driver(**kwargs):
    options = Options()
    options.add_argument('--headless')
    options.add_argument('window-size=1200x600')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    # remote_chromedriver 컨테이너에 연결
    remote_webdriver = 'http://remote_chromedriver:4444/wd/hub'
    driver = webdriver.Remote(remote_webdriver, options=options)
    driver.set_page_load_timeout(120)  # 페이지 로드 타임아웃을 120초로 설정
    driver.implicitly_wait(60)  # 암묵적 대기 시간을 60초로 설정
    
    return driver

# DAG 설정
@dag(
    dag_id="naver_news_scraping",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    tags=["scraping", "selenium"],
    catchup=False
)
def naver_news_etl():
    
    # 뉴스 제목 추출
    @task
    def scrape_naver_news():
        driver = init_driver()

        # 구글 뉴스 페이지 열기
        driver.get('https://news.naver.com/')
        news_data = []
        # 뉴스 제목 추출
        titles =  []
        for i in range(1, 6):
            title = driver.find_elements(By.XPATH, f'//*[@id="_SECTION_HEADLINE_LIST_rvr5l"]/li[{i}]/div/div/div[2]/a/strong')
            titles.append(title)
        
        news_data = []
        for title in titles:
            title_text = title.text
            title_link = title.get_attribute('href')
            news_data.append({'title': title_text, 'link': title_link})
            print(f"Title: {title_text}, Link: {title_link}")

        # WebDriver 종료
        driver.quit()

        return news_data

    # 결과 저장
    @task
    def save_results(news_data):
        # 뉴스 데이터를 파일로 저장 (예: CSV 파일로 저장)
        with open('/tmp/naver_news_titles.csv', 'w') as f:
            f.write("Title, Link\n")
            for news in news_data:
                f.write(f"{news['title']}, {news['link']}\n")
        print("Results saved to /tmp/naver_news_titles.csv")

    # 상위 10개 뉴스 출력
    @task
    def output_top_10(news_data):
        print("Top 10 News:")
        for i, news in enumerate(news_data[:5]):
            print(f"{i + 1}. Title: {news['title']}, Link: {news['link']}")

    # Task 연결
    news_data = scrape_naver_news()
    save_results(news_data)
    output_top_10(news_data)

# DAG 인스턴스 생성
dag_instance = naver_news_etl()
