"""
Скрипт для объединения всех собранных данных в один файл
"""

import os
import pandas as pd
from datetime import datetime

def merge_all_data():
    """Объединить все CSV файлы с постами и комментариями"""
    data_dir = 'data'
    
    # Находим все файлы с постами
    posts_files = [f for f in os.listdir(data_dir) if f.endswith('_posts.csv')]
    comments_files = [f for f in os.listdir(data_dir) if f.endswith('_comments.csv')]
    
    print(f"Найдено файлов с постами: {len(posts_files)}")
    print(f"Найдено файлов с комментариями: {len(comments_files)}")
    
    # Объединяем посты
    all_posts = []
    for filename in posts_files:
        filepath = os.path.join(data_dir, filename)
        try:
            df = pd.read_csv(filepath, encoding='utf-8-sig')
            all_posts.append(df)
            print(f"  Загружено постов из {filename}: {len(df)}")
        except Exception as e:
            print(f"  Ошибка при загрузке {filename}: {e}")
    
    if all_posts:
        merged_posts = pd.concat(all_posts, ignore_index=True)
        # Удаляем дубликаты по post_id
        merged_posts = merged_posts.drop_duplicates(subset=['post_id'], keep='first')
        merged_posts = merged_posts.sort_values('date')
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"data/vk_data_ALL_POSTS_{timestamp}.csv"
        merged_posts.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n✓ Объединенные посты сохранены: {output_file}")
        print(f"  Всего уникальных постов: {len(merged_posts)}")
        print(f"  Период: {merged_posts['date'].min()} - {merged_posts['date'].max()}")
    
    # Объединяем комментарии
    all_comments = []
    for filename in comments_files:
        filepath = os.path.join(data_dir, filename)
        try:
            df = pd.read_csv(filepath, encoding='utf-8-sig')
            all_comments.append(df)
            print(f"  Загружено комментариев из {filename}: {len(df)}")
        except Exception as e:
            print(f"  Ошибка при загрузке {filename}: {e}")
    
    if all_comments:
        merged_comments = pd.concat(all_comments, ignore_index=True)
        # Удаляем дубликаты по comment_id
        merged_comments = merged_comments.drop_duplicates(subset=['comment_id'], keep='first')
        merged_comments = merged_comments.sort_values('date')
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"data/vk_data_ALL_COMMENTS_{timestamp}.csv"
        merged_comments.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n✓ Объединенные комментарии сохранены: {output_file}")
        print(f"  Всего уникальных комментариев: {len(merged_comments)}")
        print(f"  Период: {merged_comments['date'].min()} - {merged_comments['date'].max()}")
    
    print("\n" + "=" * 60)
    print("ОБЪЕДИНЕНИЕ ЗАВЕРШЕНО")
    print("=" * 60)


if __name__ == "__main__":
    merge_all_data()

