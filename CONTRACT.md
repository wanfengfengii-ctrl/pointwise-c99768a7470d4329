# 有限AMQP投递至持久归档

本题不建立真实网络连接，不实现认证、消费订阅、重发发送、broker或全套AMQP。输入是AMQP 0-9-1限定帧子集，连接/通道边界由调用方明确提供。只支持class 60 basic.deliver、对应content header、content body和heartbeat。AMQP整数均网络字节序。

实现amqpingest.py。Decoder()有begin(connection_id)、open(channel)、feed(bytes)->list[dict]、close(channel)、disconnect()。connection_id为本对象未用过的非空字符串，channel为1..65535。begin仅可在无连接时调用；open只能开未开通道，为该通道每次打开从1递增generation。close允许在半条时丢弃该通道未完成内容，返回事件；disconnect丢弃所有半条，返回事件并关闭连接。feed块0..65536字节，任意分块，解析出的事件不含完整正文拼接。

每帧为type:u8、channel:u16、payload_size:u32、payload、0xCE；payload_size至多65536。type=8要求channel=0及size=0，不产事件。type=1要求channel已开；payload为class=60:u16、method=60:u16、consumer_tag:shortstr、delivery_tag:u64、redelivered:u8、exchange:shortstr、routing_key:shortstr；shortstr为u8字节长及严格UTF-8，redelivered只能0或1。delivery_tag为正整数，在同一连接和通道generation内严格递增，不要求连续。下一同通道消息只能在前条完整结束后开始。type=2对应当前deliver且只允许一次：class:u16=60、weight:u16=0、body_size:u64、property_flags:u16=0x0080、message_id:shortstr，id非空。type=3只在已收到header后合法，payload按帧顺序归该通道当前正文，累计不能超body_size。header后body_size=0立即完成；正长度最后一帧恰好凑足即完成。不支持其它帧、属性或方法，payload不能有尾随字节。未知通道、坏末字节、字段或顺序抛ValueError，Decoder永久失败；disconnect只丢残缺帧/半条，不把它们当完成。

事件字典：open/close含connection,channel,generation；disconnect含connection；begin含connection,channel,generation,tag,message_id,size；chunk含同一身份四元组(connection,channel,generation,tag)及data:bytes；end含身份四元组。header全部合法后才发begin，正文产生chunk；一次feed可返多条事件，每chunk不超过65536。open/close/disconnect方法返回自己的事件list，调用者按真实调用顺序传给Archive；禁止断开后自行把旧事件放进新连接。

Archive(db_path)使用SQLite持久存储，accept(event)->ack或None，close()；同一路径重开保留已完成消息。Archive须独立核对每个通道代和事件序列、逐条正文长度，不使用Decoder私有字段。第三方事件源需满足同样的连接、通道代和tag规则；open确立活跃通道代，close/disconnect撤销其半条。事件连接id可在Archive重开后重用，但进程内已知连接断开后不能复活。begin时tag严格递增，各通道可交错；chunk/end只能指当前那条消息，end恰好凑满才合法。

end若成功持久接纳，返回dict：connection,channel,generation,tag,frame；frame是type1、该channel、payload(class:u16=60,method:u16=80,delivery_tag:u64=tag,multiple:u8=0)、0xCE的basic.ack完整帧。其它事件返回None。ACK只表达这次投递，不能从其它通道或旧代借用，不能multiple批量确认。

message_id为全归档永久业务身份，不与通道或tag绑定。首次完整消息接纳其正文；同id正文逐字节相同可返回当前ACK而不新建业务消息，同id不同正文抛ValueError且保留原消息。redelivered不影响业务去重，不可靠它判定首次。完成消息按首次成功接纳顺序查询：Archive.items()->[(message_id,size,sha256)]；Archive.read(message_id)返回独立只读二进制文件对象，调用方负责close，未知id抛KeyError。items可驻内存；正文必须逐块使用SQLite BLOB或私有文件，不许整条攒内存。使用私有文件时须保证db重开不会暴露未落盘、缺失或被覆盖内容，允许SQLite BLOB作为更简单合法方案。重投也须完整核对内容，不得只比较id或信任输入size。

API调用单线程串行，Archive可同时持有多个通道的未完正文；额外正文内存总计至多8MiB（调用方feed块与items结果不计，内存占用不能随累计正文增长）。源断开、Archive.close和结构错误只清自己的半条，已提交消息和无关文件不变；结构错误使Archive永久失败。进程被SIGKILL后重开可清未完成私有内容，已返回ACK的业务内容必须保全；尚未返回ACK但事务已提交的消息允许已存在，重投须安全补ACK。不要求数据库损坏恢复、并发进程、通用磁盘配额或跨进程同时读取。

公开样例：连接c通道1代1的tag7接纳id=x正文abc得ACK(c,1,1,7)；随后通道1代2的tag7重投x/abc得ACK(c,1,2,7)，items仍仅x；若重投x/abd则拒绝并保持x=abc。通道2的半条、心跳及通道1正文交错不改变各自归属。

verify须用独立构造帧核对解码，同时绕过Decoder直接驱动Archive；核对正文内容、ACK完整字节和身份。真实子进程在大正文中途与持久接纳之后被杀，重开验证items/read和重投，再用生成数据流实测内存；不能仅检查自带verify退出或用程序日志代替业务断言。
交付.pointwise-runtime.json：version=1、kind=cli，setup为准备命令argv数组列表，verify/demo为执行真实业务核验及演示的argv数组。原fixture_check.py只检查公开材料，不能作为目标业务核验。
