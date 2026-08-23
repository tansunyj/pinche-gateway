package com.llmate.multiprotocol.service;

import com.llmate.multiprotocol.entity.UserUsersEntity;
import com.llmate.multiprotocol.repository.UserUsersRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * 用户服务
 */
@Service
@RequiredArgsConstructor
@Log4j2
public class UserUsersService {

    private final UserUsersRepository userUsersRepository;

    /**
     * 根据ID查找用户
     */
    public Mono<UserUsersEntity> findById(Long id) {
        return userUsersRepository.findById(id);
    }
}
