"""
Главный скрипт для запуска полного цикла:
1. Сбор данных из ВКонтакте
2. Анализ собранных данных
"""

import sys
import os

def main():
    """Главная функция для запуска полного цикла"""
    print("=" * 60)
    print("АНАЛИЗ ДАННЫХ СОЦИАЛЬНЫХ СЕТЕЙ ВКОНТАКТЕ")
    print("=" * 60)
    print("\nВыберите действие:")
    print("1. Собрать данные из ВКонтакте")
    print("2. Проанализировать собранные данные")
    print("3. Выполнить полный цикл (сбор + анализ)")
    print("0. Выход")
    
    choice = input("\nВаш выбор: ").strip()
    
    if choice == "1":
        print("\nЗапуск сбора данных...")
        from data_collector import collect_data
        collect_data()
        
    elif choice == "2":
        print("\nЗапуск анализа данных...")
        from analyzer import main as analyze_main
        analyze_main()
        
    elif choice == "3":
        print("\nЗапуск полного цикла...")
        print("\n[ШАГ 1] Сбор данных...")
        from data_collector import collect_data
        collect_data()
        
        print("\n[ШАГ 2] Анализ данных...")
        from analyzer import main as analyze_main
        analyze_main()
        
        print("\n✓ Полный цикл завершен!")
        
    elif choice == "0":
        print("Выход...")
        sys.exit(0)
        
    else:
        print("Неверный выбор. Попробуйте снова.")


if __name__ == "__main__":
    main()

