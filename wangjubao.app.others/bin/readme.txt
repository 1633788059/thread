�ع����ڼ���

�� �� ��:  deploy :223.5.23.48
�ű�Ŀ¼��/home/deploy/bin/deploy_others_buyerepay.sh
��־Ŀ¼: /data/home/deploy/output/wangjubao.app.others
�� �� ��223.4.48.193
����Ŀ¼: /home/deploy/dolphin/bin/release_others_48.sh

同步会员信息
编译主机 223.4.48.193
编译脚本: /home/deploy/dolphin/bin/release_others_buyerinfo.sh

1.批次更新  deploy : 223.5.23.48
部署脚本/home/deploy/bin/deploy_others_buyerinfo.sh
日志目录: /data/home/deploy/output/wangjubao.app.others/buyer-info.log
2.周期更新  deploy : 223.4.51.219
部署脚本/home/deploy/bin/deploy_others_buyerinfo.sh
日志目录: /data/home/deploy/output/wangjubao.app.others/buyer-info.log

�쳣����
�� �� ��:  deploy : 223.4.51.219
�ű�Ŀ¼��/home/deploy/bin/deploy_others_logistics.sh
��־Ŀ¼: /data/home/deploy/output/logs
�� �� ��223.4.48.193
����Ŀ¼: /home/deploy/dolphin/bin/release_others_219.sh

������Ϣ
�� �� ��:  deploy : 223.4.51.219
�ű�Ŀ¼��/home/deploy/bin/deploy_others_buyerinfo.sh
��־Ŀ¼: /data/home/deploy/output/logs
�� �� ��223.4.48.193
����Ŀ¼: /home/deploy/dolphin/bin/release_others_219.sh

��Ѽ�¼����
�� �� ��:  deploy : 223.4.51.219
�ű�Ŀ¼��/home/deploy/bin/deploy_others_historydata.sh
��־Ŀ¼: /data/home/deploy/output/logs
�� �� ��223.4.48.193
����Ŀ¼: /home/deploy/dolphin/bin/release_others_219.sh



���裺1) ssh��¼223.4.48.193
      2) �ϴ��ű�release_others_48.sh,release_others_219.sh������Ŀ¼/home/deploy/dolphin/bin/
      3) ���нű�����Ŀ������223.5.23.48��223.4.51.219��
      4��ssh��¼223.5.23.48
      5�����нű�deploy_others_buyerepay.sh,�����ع����ڼ���
      6) ���нű�deploy_others_buyerinfo.sh�������Ż�ȯЧ������
      7) ���Ҫ�����������нű�restartws.sh
      8��ssh��¼223.4.51.219
      9�����нű�deploy_others_logistics.sh�������쳣��������
      10�����нű�deploy_others_buyerinfo.sh������������Ϣ����
      11�����нű�deploy_others_historydata.sh��������Ѽ�¼����
      12) ���Ҫ�����������нű�restartws.sh