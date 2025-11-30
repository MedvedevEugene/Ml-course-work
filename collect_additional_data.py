"""
Скрипт для сбора дополнительных данных за предыдущие годы
Парсит данные от самой старой даты в существующих данных назад до 2020 года
"""

import os
import sys
import pandas as pd
from parser import VKParser
from datetime import datetime, timedelta
import json

# Попытка импортировать конфигурацию
try:
    from config import VK_ACCESS_TOKEN, TARGET_IDS, MAX_COMMENTS_PER_POST
except ImportError:
    print("Ошибка: Создайте файл config.py на основе config.py.example")
    sys.exit(1)


def get_oldest_date_in_data():
    """Получить самую старую дату из существующих данных"""
    data_dir = 'data'
    if not os.path.exists(data_dir):
        print("Папка data не существует. Используем текущую дату.")
        return datetime.now()
    
    # Ищем все файлы с постами
    csv_files = [f for f in os.listdir(data_dir) if f.endswith('_posts.csv')]
    
    if not csv_files:
        print("Не найдены существующие данные. Используем текущую дату.")
        return datetime.now()
    
    # Берем файл с summary или последний файл
    summary_files = [f for f in csv_files if 'summary' in f]
    if summary_files:
        latest_file = max(summary_files, key=lambda f: os.path.getmtime(os.path.join(data_dir, f)))
    else:
        latest_file = max(csv_files, key=lambda f: os.path.getmtime(os.path.join(data_dir, f)))
    
    filepath = os.path.join(data_dir, latest_file)
    
    try:
        df = pd.read_csv(filepath, encoding='utf-8-sig')
        df['date'] = pd.to_datetime(df['date'])
        oldest_date = df['date'].min()
        print(f"Найдена самая старая дата в существующих данных: {oldest_date}")
        return oldest_date
    except Exception as e:
        print(f"Ошибка при чтении файла: {e}")
        return datetime.now()


def collect_year_data(parser, target_id, owner_id, start_date, end_date, year_num):
    """Собрать данные за один год"""
    import time
    
    print(f"\n{'='*60}")
    print(f"СБОР ДАННЫХ ЗА ГОД {year_num}")
    print(f"Период: {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}")
    print(f"{'='*60}")
    
    all_posts = []
    offset = 0
    batch_size = 100
    start_timestamp = int(start_date.timestamp())
    end_timestamp = int(end_date.timestamp())
    
    print("Получаю посты...")
    
    while True:
        try:
            posts = parser.get_posts(owner_id, count=batch_size, offset=offset)
            if not posts:
                break
            
            # Фильтруем посты по дате
            filtered_posts = []
            reached_start = False
            
            for post in posts:
                post_date = post.get('date', 0)
                
                # Если пост старше начальной даты, прекращаем
                if post_date < start_timestamp:
                    reached_start = True
                    break
                
                # Если пост в нужном диапазоне
                if start_timestamp <= post_date <= end_timestamp:
                    filtered_posts.append(post)
            
            all_posts.extend(filtered_posts)
            offset += len(posts)
            
            print(f"  Собрано постов: {len(all_posts)} (offset: {offset})", flush=True)
            
            # Если достигли начальной даты или получили меньше постов - прекращаем
            if reached_start or len(posts) < batch_size:
                break
            
            # Защита от rate limiting
            time.sleep(0.35)
        except Exception as e:
            print(f"Ошибка при получении постов: {e}")
            break
    
    print(f"\n✓ Получено {len(all_posts)} постов за период")
    
    # Собираем комментарии
    all_comments = []
    if all_posts:
        print(f"\nСобираю комментарии для {len(all_posts)} постов...")
        
        for i, post in enumerate(all_posts):
            if i % 10 == 0:
                print(f"  Обработано постов: {i}/{len(all_posts)} (комментариев: {len(all_comments)})", flush=True)
            
            post_id = post.get('id')
            comments = parser.get_comments(owner_id, post_id, MAX_COMMENTS_PER_POST)
            all_comments.extend(comments)
            
            time.sleep(0.35)
    
    print(f"✓ Получено {len(all_comments)} комментариев")
    
    return {
        'posts': all_posts,
        'comments': all_comments,
        'start_date': start_date.isoformat(),
        'end_date': end_date.isoformat()
    }


def save_year_data(data, target_id, start_date, end_date):
    """Сохранить данные за год в CSV"""
    import pandas as pd
    
    # Обрабатываем посты
    posts_data = []
    for post in data['posts']:
        post_date = datetime.fromtimestamp(post.get('date', 0)) if post.get('date') else None
        posts_data.append({
            'post_id': post.get('id'),
            'target_id': target_id,
            'owner_id': f"-{post.get('owner_id', 'unknown')}" if post.get('owner_id', 0) > 0 else post.get('owner_id', 'unknown'),
            'date': post_date.strftime('%Y-%m-%d %H:%M:%S') if post_date else '',
            'date_timestamp': post.get('date', ''),
            'text': post.get('text', ''),
            'text_length': len(post.get('text', '')),
            'likes': post.get('likes', {}).get('count', 0),
            'reposts': post.get('reposts', {}).get('count', 0),
            'comments_count': post.get('comments', {}).get('count', 0),
            'views': post.get('views', {}).get('count', 0) if 'views' in post else 0,
            'engagement': (post.get('likes', {}).get('count', 0) + 
                         post.get('reposts', {}).get('count', 0) + 
                         post.get('comments', {}).get('count', 0))
        })
    
    # Обрабатываем комментарии
    comments_data = []
    for comment in data['comments']:
        comment_date = datetime.fromtimestamp(comment.get('date', 0)) if comment.get('date') else None
        comments_data.append({
            'comment_id': comment.get('id'),
            'post_id': comment.get('post_id'),
            'target_id': target_id,
            'owner_id': f"-{comment.get('owner_id', 'unknown')}" if comment.get('owner_id', 0) > 0 else comment.get('owner_id', 'unknown'),
            'date': comment_date.strftime('%Y-%m-%d %H:%M:%S') if comment_date else '',
            'date_timestamp': comment.get('date', ''),
            'text': comment.get('text', ''),
            'text_length': len(comment.get('text', '')),
            'likes': comment.get('likes', {}).get('count', 0),
            'author_id': comment.get('from_id', 0)
        })
    
    # Сохраняем CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    year_str = f"{start_date.year}"
    
    if posts_data:
        df_posts = pd.DataFrame(posts_data)
        posts_filename = f"data/vk_data_{target_id}_{year_str}_{timestamp}_posts.csv"
        df_posts.to_csv(posts_filename, index=False, encoding='utf-8-sig')
        print(f"✓ Посты сохранены: {posts_filename}")
    
    if comments_data:
        df_comments = pd.DataFrame(comments_data)
        comments_filename = f"data/vk_data_{target_id}_{year_str}_{timestamp}_comments.csv"
        df_comments.to_csv(comments_filename, index=False, encoding='utf-8-sig')
        print(f"✓ Комментарии сохранены: {comments_filename}")
    
    return posts_filename if posts_data else None, comments_filename if comments_data else None


def main():
    """Основная функция"""
    print("=" * 60)
    print("СБОР ДОПОЛНИТЕЛЬНЫХ ДАННЫХ ЗА ПРЕДЫДУЩИЕ ГОДЫ")
    print("=" * 60)
    
    # Получаем самую старую дату из существующих данных
    oldest_date = get_oldest_date_in_data()
    target_year = 2020  # Целевой год
    
    if oldest_date.year <= target_year:
        print(f"✓ Данные уже собраны до {target_year} года!")
        return
    
    # Инициализируем парсер
    parser = VKParser(VK_ACCESS_TOKEN)
    
    # Получаем информацию о группе
    target_id = TARGET_IDS[0]
    if not target_id.startswith('-') and not target_id.lstrip('-').isdigit():
        info = parser.get_group_by_screen_name(target_id)
        if info and 'owner_id' in info:
            owner_id = info['owner_id']
        else:
            print(f"Ошибка: не удалось найти группу {target_id}")
            return
    else:
        owner_id = target_id
    
    # Парсим данные по годам, начиная с самой старой даты
    current_end = oldest_date
    year_num = 1
    
    while current_end.year > target_year:
        # Определяем период для следующего года
        current_start = datetime(current_end.year - 1, 1, 1)
        if current_start.year < target_year:
            current_start = datetime(target_year, 1, 1)
        
        print(f"\n\n{'#'*60}")
        print(f"ГОД {year_num}: {current_start.year}")
        print(f"{'#'*60}")
        
        # Собираем данные за год
        year_data = collect_year_data(
            parser, target_id, owner_id, 
            current_start, current_end, year_num
        )
        
        if year_data['posts']:
            # Сохраняем данные за год
            save_year_data(year_data, target_id, current_start, current_end)
        
        # Переходим к следующему году
        current_end = current_start - timedelta(days=1)
        year_num += 1
        
        # Если достигли целевого года - прекращаем
        if current_start.year <= target_year:
            break
    
    print("\n" + "=" * 60)
    print("СБОР ДОПОЛНИТЕЛЬНЫХ ДАННЫХ ЗАВЕРШЕН")
    print("=" * 60)
    print("\nТеперь можно объединить все данные в один файл.")


if __name__ == "__main__":
    main()

