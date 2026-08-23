package com.llmate.multiprotocol.repository;

import com.llmate.multiprotocol.entity.UserUsersEntity;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

/**
 * 用户主表 Repository
 * 对应 pt_users（旧 user_users 的 name/email 列已不存在，故 findByName/findByEmail 删除）
 */
@Repository
public interface UserUsersRepository extends R2dbcRepository<UserUsersEntity, Long> {

    /**
     * 原子扣减余额：balance = balance - :amount
     * 用相对扣减而非"读出-改写"，避免并发结算互相覆盖导致丢更新。
     */
    @Modifying
    @Query("UPDATE pt_users SET balance = balance - :amount WHERE id = :userId")
    Mono<Integer> decrementBalance(@Param("userId") Long userId, @Param("amount") long amount);
}
