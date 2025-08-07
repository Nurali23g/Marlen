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
