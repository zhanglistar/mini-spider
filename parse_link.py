#!/usr/bin/python
#coding:utf-8

import HTMLParser, BeautifulSoup
import urllib, chardet

class parseLinks(HTMLParser.HTMLParser):
  def __init__(self):
    HTMLParser.HTMLParser.__init__(self)
    self.links = []
  def handle_starttag(self, tag, attrs):
    if tag == 'a':
      links = [v for k, v in attrs if k == 'href']
      if links:
        self.links.extend(links)

def linkParser_B(url):
  alllinks = []
  html = urllib.urlopen(url).read()
  info = chardet.detect(html)
  encode_str = info['encoding']
  if info['encoding'] == 'UTF-8':
    encode_str = 'utf-8'
  elif info['encoding'] == 'GB2312':
    encode_str = 'gbk'
  soup=BeautifulSoup.BeautifulStoneSoup(html.decode(encode_str))
  links=soup.findAll('a')
  alllinks = [i['href'] for i in links if('href') in str(i)]
  return alllinks

def main():
  url = 'http://www.baidu.com'
  '''
  lParser = parseLinks()
  lParser.links = []
  lParser.feed(urllib.urlopen(url).read())
  print lParser.links
  '''
  print linkParser_B(url)

if __name__ == '__main__':
  main()
