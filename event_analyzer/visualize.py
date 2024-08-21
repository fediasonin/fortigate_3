import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages


def visualize_data_to_pdf(json_list, pdf_filepath):
    df = pd.DataFrame(json_list)
    df['time'] = pd.to_datetime(df['time'])
    df['hour'] = df['time'].dt.hour

    with PdfPages(pdf_filepath) as pdf:
        fig, ax = plt.subplots(figsize=(30, 16))
        df['object.type'].value_counts().head(40).plot(kind='barh', ax=ax, color='lightgreen')
        ax.set_title('Сигнатуры', fontsize=12)
        ax.set_xlabel('Количество событий', fontsize=10)
        ax.set_ylabel('Сигнатура', fontsize=10)
        ax.tick_params(axis='both', which='major', labelsize=7)
        pdf.savefig(fig)
        plt.close(fig)

        fig, ax = plt.subplots(figsize=(30, 16))
        df['src.ip'].value_counts().head(30).plot(kind='barh', ax=ax, color='lightgreen')
        ax.set_title('Атакующие адреса', fontsize=12)
        ax.set_xlabel('Количество событий', fontsize=10)
        ax.set_ylabel('Адреса атакующих', fontsize=10)
        ax.tick_params(axis='both', which='major', labelsize=12)
        pdf.savefig(fig)
        plt.close(fig)

        fig, ax = plt.subplots(figsize=(30, 16))
        country_counts = df['src.geo.country'].value_counts()
        country_counts.plot(kind='barh', ax=ax, color='lightgreen')

        ax.set_title('Географические источники атак', fontsize=12)
        ax.set_ylabel('Страна', fontsize=10)
        ax.set_xlabel('Количество событий', fontsize=10)
        ax.tick_params(axis='both', which='major', labelsize=14)
        pdf.savefig(fig)
        plt.close(fig)

        fig, ax = plt.subplots(figsize=(30, 16))
        target_counts = df.groupby(['dst.host', 'dst.port']).size().sort_values(ascending=False).head(40)
        target_counts.plot(kind='barh', ax=ax, color='lightgreen')
        ax.set_title('Самые атакуемые ресурсы', fontsize=14)
        ax.set_xlabel('Количество событий', fontsize=12)
        ax.set_ylabel('Цели (Хост:Порт)', fontsize=12)
        ax.tick_params(axis='both', which='major', labelsize=14)
        pdf.savefig(fig)
        plt.close(fig)

        fig, ax = plt.subplots(figsize=(30, 16))
        hour_counts = df['hour'].value_counts().sort_index()
        hour_counts.plot(kind='bar', ax=ax, color='skyblue')

        for index, value in enumerate(hour_counts):
            ax.text(index, value, str(value), ha='center', va='bottom')

        ax.set_title('Распределение событий по часам (UTC +3)', fontsize=12)
        ax.set_xlabel('Время', fontsize=14)
        ax.set_ylabel('Количество событий', fontsize=14)
        ax.tick_params(axis='both', which='major', labelsize=14)
        pdf.savefig(fig)
        plt.close(fig)
