# SqliteHelper
sqlite 多线程操作线程安全
参考：https://www.cnblogs.com/blqw/p/3475734.html 让C#轻松实现读写锁分离－－封装ReaderWriterLockSlim
     https://www.iteye.com/blog/yacki-1967119     C#使用读写锁解决SQLITE并发异常问题
     结合2者的优势，写了个一个sqlite多线程操作，读写分离的方式写入读取数据库
