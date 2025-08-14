SELECT TO_DATE(
  REPLACE(
    REPLACE(
      REPLACE(
        REPLACE(
          REPLACE(
            REPLACE(
              REPLACE(
                REPLACE(
                  REPLACE(
                    REPLACE(
                      REPLACE(
                        REPLACE(
                          REGEXP_REPLACE('В halyk maeket с 12 декабря, 2020 г.', '.*?(\d{1,2} [а-яА-Я]+), (\d{4})', '\1 \2'),
                          'января', 'January'),
                        'февраля', 'February'),
                      'марта', 'March'),
                    'апреля', 'April'),
                  'мая', 'May'),
                'июня', 'June'),
              'июля', 'July'),
            'августа', 'August'),
          'сентября', 'September'),
        'октября', 'October'),
      'ноября', 'November'),
    'декабря', 'December'),
  'DD Month YYYY') AS parsed_date;



создал парсеры для  Forte, Kaspi, Halyk и Jusan market. которые быстро собирают ID, название продукта, цену, ссылку и время запуска. парсеры созданы для запуска несколько раз за день.
обновил основные парсеры по Kaspi, Halyk и Jusan market. они используют ID из предыдущего парсера.
