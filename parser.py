"""
Парсер данных из социальной сети ВКонтакте
Использует VK API для получения постов, комментариев и метаданных
"""

import vk_api
import time
import json
import pandas as pd
from typing import List, Dict, Optional
from datetime import datetime, timedelta


class VKParser:
    """Класс для парсинга данных из ВКонтакте"""
    
    def __init__(self, access_token: str):
        """
        Инициализация парсера
        
        Args:
            access_token: Токен доступа VK API
        """
        self.vk_session = vk_api.VkApi(token=access_token)
        self.vk = self.vk_session.get_api()
        
    def get_group_by_screen_name(self, screen_name: str) -> Dict:
        """
        Получить информацию о группе по короткому имени (screen_name)
        
        Args:
            screen_name: Короткое имя группы (например, 'minzdravru')
            
        Returns:
            Словарь с информацией о группе, включая ID
        """
        try:
            # Убираем @ и / если есть
            screen_name = screen_name.lstrip('@/')
            groups = self.vk.groups.getById(group_id=screen_name)
            if groups:
                group_info = groups[0]
                # Возвращаем ID с минусом для дальнейшей работы
                group_info['owner_id'] = f"-{group_info['id']}"
                return group_info
        except Exception as e:
            print(f"Ошибка при получении информации о группе {screen_name}: {e}")
        return {}
    
    def get_group_info(self, group_id: str) -> Dict:
        """
        Получить информацию о группе
        
        Args:
            group_id: ID группы (с минусом или без)
            
        Returns:
            Словарь с информацией о группе
        """
        try:
            # Убираем минус для запроса
            clean_id = group_id.lstrip('-')
            groups = self.vk.groups.getById(group_id=clean_id)
            if groups:
                return groups[0]
        except Exception as e:
            print(f"Ошибка при получении информации о группе {group_id}: {e}")
        return {}
    
    def get_user_info(self, user_id: str) -> Dict:
        """
        Получить информацию о пользователе
        
        Args:
            user_id: ID пользователя
            
        Returns:
            Словарь с информацией о пользователе
        """
        try:
            users = self.vk.users.get(user_ids=user_id, fields='city,country,sex,bdate')
            if users:
                return users[0]
        except Exception as e:
            print(f"Ошибка при получении информации о пользователе {user_id}: {e}")
        return {}
    
    def get_posts(self, owner_id: str, count: int = 100, offset: int = 0) -> List[Dict]:
        """
        Получить посты из группы или со стены пользователя
        
        Args:
            owner_id: ID владельца (группа с минусом или пользователь)
            count: Количество постов для получения
            offset: Смещение для пагинации
            
        Returns:
            Список постов
        """
        try:
            posts = self.vk.wall.get(
                owner_id=owner_id,
                count=min(count, 100),  # VK API ограничивает до 100 за раз
                offset=offset,
                extended=0
            )
            return posts.get('items', [])
        except Exception as e:
            print(f"Ошибка при получении постов для {owner_id}: {e}")
            return []
    
    def get_all_posts(self, owner_id: str, max_posts: int = 100, 
                     start_date: Optional[datetime] = None, 
                     end_date: Optional[datetime] = None) -> List[Dict]:
        """
        Получить все посты с учетом ограничений API и фильтрации по дате
        
        Args:
            owner_id: ID владельца
            max_posts: Максимальное количество постов
            start_date: Начальная дата для фильтрации (если None - без ограничения)
            end_date: Конечная дата для фильтрации (если None - без ограничения)
            
        Returns:
            Список всех постов, отфильтрованных по дате
        """
        all_posts = []
        offset = 0
        batch_size = 100
        
        # Конвертируем даты в timestamp если указаны
        start_timestamp = int(start_date.timestamp()) if start_date else None
        end_timestamp = int(end_date.timestamp()) if end_date else None
        
        while len(all_posts) < max_posts:
            posts = self.get_posts(owner_id, count=batch_size, offset=offset)
            if not posts:
                break
            
            # Фильтруем посты по дате
            filtered_posts = []
            for post in posts:
                post_date = post.get('date', 0)
                
                # Если пост старше начальной даты, прекращаем (посты идут от новых к старым)
                if start_timestamp and post_date < start_timestamp:
                    # Если мы уже прошли нужный период, прекращаем сбор
                    return all_posts[:max_posts]
                
                # Проверяем, попадает ли пост в нужный диапазон
                if start_timestamp and post_date < start_timestamp:
                    continue
                if end_timestamp and post_date > end_timestamp:
                    continue
                
                filtered_posts.append(post)
            
            all_posts.extend(filtered_posts)
            offset += len(posts)
            
            # Защита от rate limiting
            time.sleep(0.35)  # VK API позволяет 3 запроса в секунду
            
            # Если получили меньше постов чем запрашивали, значит достигли конца
            if len(posts) < batch_size:
                break
            
            # Если после фильтрации не осталось постов, но мы еще не достигли нужного периода
            # продолжаем искать дальше
            if not filtered_posts and start_timestamp:
                # Проверяем, не ушли ли мы слишком далеко назад
                oldest_post_date = min(p.get('date', 0) for p in posts) if posts else 0
                if oldest_post_date < start_timestamp:
                    break
        
        return all_posts[:max_posts]
    
    def get_comments(self, owner_id: str, post_id: int, max_comments: int = 50) -> List[Dict]:
        """
        Получить комментарии к посту
        
        Args:
            owner_id: ID владельца поста
            post_id: ID поста
            max_comments: Максимальное количество комментариев
            
        Returns:
            Список комментариев
        """
        try:
            comments = self.vk.wall.getComments(
                owner_id=owner_id,
                post_id=post_id,
                count=min(max_comments, 100),
                extended=0,
                need_likes=1
            )
            return comments.get('items', [])
        except Exception as e:
            print(f"Ошибка при получении комментариев для поста {post_id}: {e}")
            return []
    
    def get_post_likes(self, owner_id: str, post_id: int) -> Dict:
        """
        Получить информацию о лайках поста
        
        Args:
            owner_id: ID владельца поста
            post_id: ID поста
            
        Returns:
            Словарь с информацией о лайках
        """
        try:
            likes = self.vk.likes.getList(
                type='post',
                owner_id=owner_id,
                item_id=post_id,
                count=1000
            )
            return likes
        except Exception as e:
            print(f"Ошибка при получении лайков для поста {post_id}: {e}")
            return {}
    
    def parse_target(self, target_id: str, max_posts: int = 100, max_comments: int = 50,
                    years_back: int = None) -> Dict:
        """
        Полный парсинг целевого объекта (группы или пользователя)
        
        Args:
            target_id: ID цели (группа с минусом, пользователь или screen_name группы)
            max_posts: Максимальное количество постов
            max_comments: Максимальное количество комментариев на пост
            years_back: Количество лет назад для фильтрации (если None - без ограничения)
            
        Returns:
            Словарь с собранными данными
        """
        print(f"Начинаю парсинг {target_id}...", flush=True)
        
        # Проверяем, является ли target_id screen_name (не начинается с минуса и не число)
        owner_id = target_id
        info = {}
        
        if not target_id.startswith('-') and not target_id.lstrip('-').isdigit():
            # Это screen_name, получаем информацию о группе
            print(f"Определен screen_name: {target_id}", flush=True)
            info = self.get_group_by_screen_name(target_id)
            if info and 'owner_id' in info:
                owner_id = info['owner_id']
                print(f"Найден ID группы: {owner_id}", flush=True)
            else:
                print(f"Ошибка: не удалось найти группу {target_id}", flush=True)
                return {}
        else:
            # Это ID, получаем информацию обычным способом
            is_group = target_id.startswith('-')
            clean_id = target_id.lstrip('-')
            
            if is_group:
                info = self.get_group_info(target_id)
            else:
                info = self.get_user_info(clean_id)
        
        # Определяем диапазон дат для фильтрации
        end_date = datetime.now()
        start_date = None
        if years_back:
            start_date = end_date - timedelta(days=years_back * 365)
            print(f"Фильтрация постов за период: {start_date.strftime('%Y-%m-%d')} - {end_date.strftime('%Y-%m-%d')}")
        
        # Получаем посты
        print(f"Получаю посты...", flush=True)
        posts = self.get_all_posts(owner_id, max_posts, start_date=start_date, end_date=end_date)
        print(f"Получено {len(posts)} постов", flush=True)
        
        # Получаем комментарии для каждого поста
        all_comments = []
        print(f"Начинаю сбор комментариев для {len(posts)} постов...", flush=True)
        for i, post in enumerate(posts):
            if i % 10 == 0 or i == 0:
                print(f"Обработано постов: {i}/{len(posts)} (комментариев собрано: {len(all_comments)})", flush=True)
            
            post_id = post.get('id')
            comments = self.get_comments(owner_id, post_id, max_comments)
            all_comments.extend(comments)
            
            time.sleep(0.35)  # Защита от rate limiting
        
        print(f"Получено {len(all_comments)} комментариев", flush=True)
        
        return {
            'target_id': target_id,
            'owner_id': owner_id,
            'target_info': info,
            'posts': posts,
            'comments': all_comments,
            'parsed_at': datetime.now().isoformat(),
            'date_range': {
                'start': start_date.isoformat() if start_date else None,
                'end': end_date.isoformat()
            }
        }
    
    def save_to_json(self, data: Dict, filename: str):
        """
        Сохранить данные в JSON файл
        
        Args:
            data: Данные для сохранения
            filename: Имя файла
        """
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Данные сохранены в {filename}")
    
    def save_to_csv(self, data: Dict, base_filename: str):
        """
        Сохранить данные в CSV файлы (отдельно посты и комментарии)
        
        Args:
            data: Данные для сохранения
            base_filename: Базовое имя файла (без расширения)
        """
        posts_data = []
        comments_data = []
        
        target_id = data.get('target_id', 'unknown')
        owner_id = data.get('owner_id', 'unknown')
        posts = data.get('posts', [])
        comments = data.get('comments', [])
        
        # Обрабатываем посты
        for post in posts:
            post_date = datetime.fromtimestamp(post.get('date', 0)) if post.get('date') else None
            posts_data.append({
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
        for comment in comments:
            comment_date = datetime.fromtimestamp(comment.get('date', 0)) if comment.get('date') else None
            comments_data.append({
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
        
        # Сохраняем посты в CSV
        if posts_data:
            df_posts = pd.DataFrame(posts_data)
            posts_filename = f"{base_filename}_posts.csv"
            df_posts.to_csv(posts_filename, index=False, encoding='utf-8-sig')
            print(f"✓ Посты сохранены в {posts_filename} ({len(posts_data)} записей)")
        
        # Сохраняем комментарии в CSV
        if comments_data:
            df_comments = pd.DataFrame(comments_data)
            comments_filename = f"{base_filename}_comments.csv"
            df_comments.to_csv(comments_filename, index=False, encoding='utf-8-sig')
            print(f"✓ Комментарии сохранены в {comments_filename} ({len(comments_data)} записей)")
        
        return posts_filename if posts_data else None, comments_filename if comments_data else None

