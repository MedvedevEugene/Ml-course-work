"""
Скрипт для сбора данных из ВКонтакте
Использует парсер для получения данных и сохранения их в файлы
"""

import os
import sys
import pandas as pd
from parser import VKParser
from datetime import datetime

# Попытка импортировать конфигурацию
try:
    from config import VK_ACCESS_TOKEN, TARGET_IDS, MAX_POSTS_PER_GROUP, MAX_COMMENTS_PER_POST
    try:
        from config import YEARS_BACK
    except ImportError:
        YEARS_BACK = None  # По умолчанию без ограничения по годам
except ImportError:
    print("Ошибка: Создайте файл config.py на основе config.py.example")
    print("И заполните его своими данными VK API")
    sys.exit(1)


def create_data_directory():
    """Создать директорию для данных, если её нет"""
    if not os.path.exists('data'):
        os.makedirs('data')
        print("Создана директория 'data' для хранения результатов")


def collect_data():
    """Основная функция для сбора данных"""
    print("=" * 50)
    print("Сбор данных из ВКонтакте")
    print("=" * 50)
    
    # Проверка токена
    if not VK_ACCESS_TOKEN or VK_ACCESS_TOKEN == "your_vk_access_token_here":
        print("Ошибка: Укажите VK_ACCESS_TOKEN в config.py")
        return
    
    # Проверка целевых ID
    if not TARGET_IDS:
        print("Ошибка: Укажите TARGET_IDS в config.py")
        return
    
    # Создаем директорию для данных
    create_data_directory()
    
    # Инициализируем парсер
    parser = VKParser(VK_ACCESS_TOKEN)
    
    # Собираем данные для каждого целевого объекта
    all_data = []
    
    for target_id in TARGET_IDS:
        print(f"\nОбработка цели: {target_id}")
        print("-" * 50)
        print(f"Параметры:")
        print(f"  - Максимум постов: {MAX_POSTS_PER_GROUP}")
        print(f"  - Максимум комментариев на пост: {MAX_COMMENTS_PER_POST}")
        print(f"  - Период: {YEARS_BACK} лет" if YEARS_BACK else "  - Период: без ограничения")
        print("-" * 50)
        
        try:
            # Парсим данные
            print("Начинаю парсинг...")
            data = parser.parse_target(
                target_id=target_id,
                max_posts=MAX_POSTS_PER_GROUP,
                max_comments=MAX_COMMENTS_PER_POST,
                years_back=YEARS_BACK
            )
            
            if not data or not data.get('posts'):
                print("⚠ Предупреждение: Данные не получены или пусты")
                continue
            
            # Сохраняем в CSV файлы
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_filename = f"data/vk_data_{target_id}_{timestamp}"
            
            print(f"\nСохраняю данные в CSV файлы...", flush=True)
            posts_file, comments_file = parser.save_to_csv(data, base_filename)
            
            # Также сохраняем JSON для резервной копии
            json_filename = f"{base_filename}.json"
            parser.save_to_json(data, json_filename)
            
            print(f"\n✓ Файлы сохранены:")
            if posts_file:
                print(f"  - {posts_file}")
            if comments_file:
                print(f"  - {comments_file}")
            print(f"  - {json_filename}")
            
            all_data.append(data)
            
            print(f"\n✓ Успешно собраны данные для {target_id}")
            
        except Exception as e:
            print(f"✗ Ошибка при сборе данных для {target_id}: {e}")
            continue
    
    # Сохраняем сводные CSV файлы со всеми данными
    if all_data:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Объединяем все посты и комментарии
        all_posts_data = []
        all_comments_data = []
        
        for item in all_data:
            target_id = item.get('target_id', 'unknown')
            owner_id = item.get('owner_id', 'unknown')
            
            # Обрабатываем посты
            for post in item.get('posts', []):
                post_date = datetime.fromtimestamp(post.get('date', 0)) if post.get('date') else None
                all_posts_data.append({
                    'post_id': post.get('id'),
                    'target_id': target_id,
                    'owner_id': owner_id,
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
            for comment in item.get('comments', []):
                comment_date = datetime.fromtimestamp(comment.get('date', 0)) if comment.get('date') else None
                all_comments_data.append({
                    'comment_id': comment.get('id'),
                    'post_id': comment.get('post_id'),
                    'target_id': target_id,
                    'owner_id': owner_id,
                    'date': comment_date.strftime('%Y-%m-%d %H:%M:%S') if comment_date else '',
                    'date_timestamp': comment.get('date', ''),
                    'text': comment.get('text', ''),
                    'text_length': len(comment.get('text', '')),
                    'likes': comment.get('likes', {}).get('count', 0),
                    'author_id': comment.get('from_id', 0)
                })
        
        # Сохраняем сводные CSV файлы
        import pandas as pd
        
        if all_posts_data:
            df_all_posts = pd.DataFrame(all_posts_data)
            summary_posts_filename = f"data/vk_data_summary_posts_{timestamp}.csv"
            df_all_posts.to_csv(summary_posts_filename, index=False, encoding='utf-8-sig')
            print(f"\n✓ Сводный файл постов сохранен: {summary_posts_filename} ({len(all_posts_data)} записей)")
        
        if all_comments_data:
            df_all_comments = pd.DataFrame(all_comments_data)
            summary_comments_filename = f"data/vk_data_summary_comments_{timestamp}.csv"
            df_all_comments.to_csv(summary_comments_filename, index=False, encoding='utf-8-sig')
            print(f"✓ Сводный файл комментариев сохранен: {summary_comments_filename} ({len(all_comments_data)} записей)")
        
        # Также сохраняем JSON для резервной копии
        summary_json_filename = f"data/vk_data_summary_{timestamp}.json"
        summary = {
            'collected_at': datetime.now().isoformat(),
            'targets_count': len(all_data),
            'data': all_data
        }
        parser.save_to_json(summary, summary_json_filename)
        
        # Выводим статистику
        print("\n" + "=" * 50)
        print("СТАТИСТИКА СБОРА ДАННЫХ")
        print("=" * 50)
        total_posts = sum(len(d['posts']) for d in all_data)
        total_comments = sum(len(d['comments']) for d in all_data)
        print(f"Всего обработано целей: {len(all_data)}")
        print(f"Всего собрано постов: {total_posts}")
        print(f"Всего собрано комментариев: {total_comments}")
        print("=" * 50)


if __name__ == "__main__":
    collect_data()

