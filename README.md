# Cryptocurrencies correlation with social media

## Description
This project aims to analyze media contents (like news text, tweets, reddit post and telegram messages) to measure correlation with Bitcoin price over time.
At the moment the project is not ready to be used with real-time data, therefore it can't be integrated with a bot application. 

## Future of the project
In the free-time, the project will be updated to support real-time analysis; probabily will be integrated in a bot application.
For real-time compatibility, A 

## Collaborations
At the moment the project is closed to direct collaborations, obviously forks are welcommed.

## Flair
This project uses Flair framework for text classification; as requested we cite two papers:

    akbik2018coling,
    title={Contextual String Embeddings for Sequence Labeling},
    author={Akbik, Alan and Blythe, Duncan and Vollgraf, Roland},
    booktitle = {{COLING} 2018, 27th International Conference on Computational Linguistics},
    pages     = {1638--1649},
    year      = {2018}


    akbik2019flair,
    title={FLAIR: An easy-to-use framework for state-of-the-art NLP},
    author={Akbik, Alan and Bergmann, Tanja and Blythe, Duncan and Rasul, Kashif and Schweter, Stefan and Vollgraf, Roland},
    booktitle={{NAACL} 2019, 2019 Annual Conference of the North American Chapter of the Association for Computational Linguistics (Demonstrations)},
    pages={54--59},
    year={2019}

## Data sources

### Twitter
- df_Final: https://www.kaggle.com/jaimebadiola/bitcoin-tweets-and-price
- tweets: https://www.kaggle.com/alaix14/bitcoin-tweets-20160101-to-20190329
- cleanprep: https://www.kaggle.com/paul92s/bitcoin-tweets-14m
- bitcoin_tweets: https://www.kaggle.com/kaushiksuresh147/bitcoin-tweets
- daily_example: https://data.world/mercal/btc-tweets-sentiment
- twitter_emotion: https://www.kaggle.com/huseinzol05/twitter-emotion-cryptocurrency
- twitter2: https://www.kaggle.com/gwhittington/twitter2
- BitcoinTweets: https://www.kaggle.com/augiedoebling/bitcoin-tweets?select=BitcoinTweets.csv
- bitcoin-twitter: https://www.kaggle.com/gwhittington/bitcointwitter?select=bitcoin-twitter.csv
- twits_BTC: https://www.kaggle.com/mohammedlaidtadjine/9000-tweets-about-bitcoin-910-dec-20220
- bitcoin: https://www.kaggle.com/liurui/tweets-about-bitcoin?select=bitcoin.csv
- bitcoin oct 17 to oct 18: https://data.mendeley.com/datasets/chx9mdyydb/1

### Reddit
- bitcoin_reddit_all.csv:  https://www.kaggle.com/jerryfanelli/reddit-comments-containing-bitcoin-2009-to-2019

### Telegram
- crypto_telegram_groups: https://www.kaggle.com/aagghh/crypto-telegram-groups
- tele_btc_messages: https://www.kaggle.com/dcaichara/telegram-messages-related-to-bitcoin

### News
- crypto_news: https://www.kaggle.com/kashnitsky/news-about-major-cryptocurrencies-20132018-40k
- cointelegraph_news: https://www.kaggle.com/asahicantu/cryptocurency-cointelegraph-newsfeed
- Headline_Crypto: https://www.kaggle.com/geraldm/headlines-for-major-crypto-from-2012-until-today?select=Headline_Crypto.csv

### Market
- btcusd.csv: https://www.kaggle.com/tencars/392-crypto-currency-pairs-at-minute-resolution

## Articles
- Correlation analysis techniques: https://towardsdatascience.com/four-ways-to-quantify-synchrony-between-time-series-data-b99136c4a9c9
- Skip-Gram and CIBOW: https://towardsdatascience.com/introduction-to-word-embedding-and-word2vec-652d0c2060fa
- RNN: https://aditi-mittal.medium.com/understanding-rnn-and-lstm-f7cdf6dfc14e

