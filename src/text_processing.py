import re
import string

import nltk
from nltk.corpus import stopwords
from nltk.corpus import wordnet
from nltk.stem import WordNetLemmatizer

from resources.emo_unicode import *
from resources.contract_word import *
from src.abbreviation_manager import AbbreviationManager


class TextProcessing:
    PUNCTUATION = string.punctuation
    STOPWORDS = [*set(stopwords.words('english'))]
    SPECIAL_CHAR = ["“", "”", "—", "–", "’", "🤣", "🥺"]
    # TODO
    #   Remove special characters and populate the SPECIAL_CHAR variable

    lemmatizer = WordNetLemmatizer()
    wordnet_map = {'N': wordnet.NOUN, 'V': wordnet.VERB, 'J': wordnet.ADJ, 'R': wordnet.ADV}

    user_tag_pattern = re.compile(r'@[\w_]+')
    hash_tag_pattern = re.compile(r'#[\w_]+')
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               u"\U00002702-\U000027B0"
                               u"\U000024C2-\U0001F251"
                               "]+", flags=re.UNICODE)
    emoticon_pattern = re.compile(u'(' + u'|'.join(k for k in EMOTICONS) + u')')
    url_pattern = re.compile(r'(https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.'
                             r'[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))'
                             r'[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,})')
    html_pattern = re.compile(r'<.*?>')
    double_space_pattern = re.compile(r' +')

    common_abbr = AbbreviationManager.get_instance().common_abbr
    financial_abbr = AbbreviationManager.get_instance().financial_abbr

    @staticmethod
    def lower_case(tweet):
        tweet['text'] = tweet['text'].lower()
        return tweet

    @staticmethod
    def remove_punctuation(tweet):
        tweet['text'] = tweet['text'].translate(str.maketrans('', '', TextProcessing.PUNCTUATION))
        return tweet

    @staticmethod
    def remove_stop_word(tweet):
        tweet['text'] = " ".join([word for word in str(tweet['text']).split() if word not in TextProcessing.STOPWORDS])
        return tweet

    @staticmethod
    def lemmatize(tweet):
        pos_tagged_text = nltk.pos_tag(tweet['text'].split())
        tweet['text'] = " ".join(
            [TextProcessing.lemmatizer.lemmatize(word, TextProcessing.wordnet_map.get(pos[0], wordnet.NOUN)) for
             word, pos in pos_tagged_text])
        return tweet

    @staticmethod
    def remove_emojis(tweet):
        tweet['text'] = TextProcessing.emoji_pattern.sub(r' ', tweet['text'])
        return tweet

    # TODO
    #   Replace text by tweet
    @staticmethod
    def emojis_to_word(text):
        for emojis in UNICODE_EMO:
            text = re.sub(r'(' + emojis + ')', "_".join(UNICODE_EMO[emojis].replace(",", "").replace(":", "").split()),
                          text)
        return text

    @staticmethod
    def remove_emoticons(tweet):
        tweet['text'] = TextProcessing.emoticon_pattern.sub(r' ', tweet['text'])
        return tweet

    # TODO
    #   Replace text by tweet
    @staticmethod
    def emoticon_to_word(text):
        for emoticon in EMOTICONS:
            text = re.sub(u'(' + emoticon + ')', "_".join(EMOTICONS[emoticon].replace(",", "").split()), text)
        return text

    @staticmethod
    def remove_url(tweet):
        tweet['text'] = TextProcessing.url_pattern.sub(r' ', tweet['text'])
        return tweet

    @staticmethod
    def remove_html_tag(tweet):
        tweet['text'] = TextProcessing.html_pattern.sub(r' ', tweet['text'])
        return tweet

    @staticmethod
    def remove_double_space(tweet):
        tweet['text'] = TextProcessing.double_space_pattern.sub(r' ', tweet['text'])
        return tweet

    @staticmethod
    def remove_user_tag(tweet):
        tweet['text'] = TextProcessing.user_tag_pattern.sub(r' ', tweet['text'])
        return tweet

    @staticmethod
    def remove_hash_tag(tweet):
        tweet['text'] = TextProcessing.hash_tag_pattern.sub(r' ', tweet['text'])
        return tweet

    @staticmethod
    def expand_contract_word(tweet):
        new_text = []
        for word in tweet['text'].split():
            if word in CONTRACT_WORDS.keys():
                new_text.append(CONTRACT_WORDS[word])
            else:
                new_text.append(word)
        tweet['text'] = ' '.join(new_text)
        return tweet

    @staticmethod
    def expand_abbreviation(tweet):
        new_text = []
        for word in tweet['text'].split():
            if word in TextProcessing.common_abbr.keys():
                new_text.append(TextProcessing.common_abbr[word])
            elif word in TextProcessing.financial_abbr.keys():
                new_text.append(TextProcessing.financial_abbr[word])
            else:
                new_text.append(word)
        tweet['text'] = ' '.join(new_text)
        return tweet

    @staticmethod
    def correct_spelling(text):
        return text
