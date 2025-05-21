# 股票数据采集系统

这是一个用于采集股票数据的系统，支持同步和异步数据采集，具有高效的数据处理和数据库存储功能。系统严格遵循robots协议，确保对目标网站的友好访问。

## 功能特点

- 从东方财富网获取股票数据
- 支持同步和异步数据采集
- 使用内存缓存优化数据获取
- 将数据存储到MySQL数据库
- 支持批量数据插入
- 使用连接池优化数据库连接
- 优化的数据类型处理
- 支持环境变量配置
- 完善的错误处理和重试机制
- 严格遵循robots协议

## 环境要求

- Python 3.7+
- MySQL 5.7+

## 安装步骤

1. 克隆项目到本地：
```bash
git clone <repository_url>
cd pystock_DC
```

2. 安装依赖包：
```bash
pip install -r requirements.txt
```

3. 配置环境变量：
   - 复制 `.env.example` 文件为 `.env`
   - 修改 `.env` 文件中的配置信息

4. 初始化数据库：
```bash
python init_database.py
```

5. 运行数据采集：
   - 同步模式：
   ```bash
   python stock_simple.py
   ```
   - 异步模式：
   ```bash
   python stock_async.py
   ```

## 配置说明

在 `.env` 文件中配置以下参数：

```ini
# MySQL配置
MYSQL_HOST=your_host
MYSQL_USER=your_user
MYSQL_PASSWORD=your_password
MYSQL_DATABASE=your_database
```

## 数据字段说明

股票数据表包含以下字段：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| stock_code | VARCHAR(10) | 股票代码 |
| stock_name | VARCHAR(100) | 股票名称 |
| current_price | DECIMAL(10,2) | 当前价格 |
| change_percent | DECIMAL(10,2) | 涨跌幅 |
| change_amount | DECIMAL(10,2) | 涨跌额 |
| volume | BIGINT | 成交量 |
| amount | DECIMAL(20,2) | 成交额 |
| amplitude | DECIMAL(10,2) | 振幅 |
| turnover_rate | DECIMAL(10,2) | 换手率 |
| volume_ratio | DECIMAL(10,2) | 量比 |
| high_price | DECIMAL(10,2) | 最高价 |
| low_price | DECIMAL(10,2) | 最低价 |
| open_price | DECIMAL(10,2) | 开盘价 |
| pre_close | DECIMAL(10,2) | 昨收价 |
| pe_ratio | DECIMAL(20,2) | 市盈率 |
| pb_ratio | DECIMAL(20,2) | 市净率 |
| update_time | DATETIME | 更新时间 |

## 优化特性

1. 内存缓存
   - 使用Python的`functools.lru_cache`进行数据缓存
   - 缓存最近100页的数据请求
   - 减少重复API调用

2. 数据库优化
   - 使用连接池管理数据库连接
   - 增加连接池大小到10
   - 批量插入大小增加到50条记录
   - 优化的数据类型处理

3. 异步处理
   - 支持并发数据采集
   - 可配置的并发数量
   - 智能的重试机制
   - 随机延迟避免请求过快

4. 错误处理
   - 完善的异常捕获和处理
   - 自动重试机制
   - 详细的日志记录
   - 数据验证和清理

## Robots协议遵循

系统严格遵循robots协议，确保对目标网站的友好访问：

1. 请求频率控制
   - 同步模式：单线程顺序请求
   - 异步模式：控制并发数量（默认3个并发）
   - 随机延迟：每次请求间隔1-3秒

2. 请求头设置
   - 设置User-Agent标识
   - 模拟正常浏览器行为
   - 避免频繁请求同一资源

3. 数据缓存
   - 使用内存缓存减少重复请求
   - 缓存最近100页的数据
   - 避免频繁请求相同数据

4. 错误处理
   - 请求失败时自动重试
   - 遇到限制时自动降低请求频率
   - 记录详细的访问日志

## 注意事项

1. 确保MySQL服务器允许远程连接
2. 建议在生产环境中使用更强的密码
3. 定期备份数据库数据
4. 注意API请求频率限制
5. 确保网络连接稳定
6. 遵守目标网站的robots协议
7. 合理设置并发数量和请求间隔

## 常见问题

1. 数据采集中断
   - 检查网络连接
   - 查看日志文件
   - 确认数据库连接正常
   - 检查是否触发网站访问限制

2. 数据不完整
   - 检查API响应
   - 验证数据格式
   - 确认字段映射正确
   - 检查请求是否被限制

3. 性能问题
   - 调整并发数量
   - 优化数据库索引
   - 检查系统资源使用
   - 适当增加请求间隔

## 许可证

MIT License 