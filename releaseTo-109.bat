echo "��������Ŀ��˽��"
echo "��ǰ�ű�Ŀ¼�� %~dp0"
 
mvn deploy clean

rem ������ʱ 
ping 127.0.0.1 -n 4 > %temp%/temp.txt
del %temp%\temp.txt
