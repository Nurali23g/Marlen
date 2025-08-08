SELECT 
    s.seller_name,
    s.category AS category_en,
    t.category_ru
FROM 
    sellers s
JOIN (
    VALUES 
        ('noutbuki-i-kompyuteri', 'Ноутбуки и компьютеры'),
        ('tehnika-dlya-doma', 'Техника для дома'),
        ('kuhonnaya-tehnika', 'Кухонная техника'),
        ('mebel', 'Мебель'),
        ('telefoni-i-gadzheti', 'Телефоны и гаджеты'),
        ('televizori-i-audiotehnika', 'ТВ, Аудио, Видео'),
        ('foto-i-video', 'Фото и видео'),
        ('tovari-dlya-doma', 'Товары для дома и дачи'),
        ('detskie-tovari', 'Детские товары'),
        ('krasota-i-zdorove', 'Красота и здоровье')
) AS t(category_en, category_ru)
ON s.category = t.category_en;
