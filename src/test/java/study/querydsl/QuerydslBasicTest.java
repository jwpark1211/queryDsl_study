package study.querydsl;

import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.jpa.impl.JPAQueryFactory;
import jakarta.persistence.EntityManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.entity.Member;
import study.querydsl.entity.Team;

import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static study.querydsl.entity.QMember.*;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {

    @Autowired
    EntityManager em;

    @BeforeEach
    public void before() {
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);
        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);
        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
    }

    @Test
    public void startJPQL(){
        //member1을 찾아라
        String qlString = "select m from Member m where m.username = :username";

        Member findMember = em.createQuery(qlString, Member.class)
                .setParameter("username","member1")
                .getSingleResult();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void startQuerydsl(){
        /*JPAQueryFactory에서는 여러 스레드에서 동시에 같은 em에 접근해도 트랜잭션마다 별도의 영속성 컨텍스트를
        제공하기 때문에 동시성 문제는 발생하지 않는다.*/
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        Member findMember = queryFactory
                .select(member)
                .from(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    /* 검색 조건 쿼리
     * 검색 조건은 .and(), .or()를 메서드체인(...)으로 연결할 수 있음
     * 참고 : select + from 을 selectFrom으로 사용할 수 있음
     */
    @Test
    public void search() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);
        Member findMember = queryFactory
                .selectFrom(member)
                //.where(member.username.eq("member1").and(member.age.eq(10)))과 동일함
                //아래 코드는 메서드체인을 통해 연결한 것!
                .where(member.username.eq("member1"), member.age.eq(10))
                .fetchOne();

        /* 결과 조회
        * fetch() : 리스트 조회, 데이터 없으면 빈 리스트 반환
        * fetchOne() : 단건 조회, 결과 없으면 null , 둘 이상이면 nonUniqueResultException
        * fetchFirst() : limit(1).fetchOne();
        * fetchResults() : 페이징 정보 포함, total count 쿼리 추가 실행
        * fetchCount() : count 쿼리로 변경해서 count 수 조회
        * */

        assertThat(findMember.getUsername()).isEqualTo("member1");

        //JPQL이 제공하는 모든 검색 조건
        member.username.eq("member1"); //equal
        member.username.ne("member1"); //not equal
        member.username.eq("member1").not(); //not equal (위와 동일)

        member.username.isNotNull(); //not null

        member.age.in(10,20); //age값이 10 또는 20
        member.age.notIn(10,20); //age값이 10 또는 20에 포함X
        member.age.between(10,30);

        member.age.goe(30); // >=
        member.age.gt(30); // >
        member.age.loe(30); // <=
        member.age.lt(30); // <

        member.username.like("member%"); //like 검색
        member.username.contains("member"); //like %member% 검색
        member.username.startsWith("member"); //like member% 검색
    }

    /** 정렬
     * 회원 정렬 순서
     * 1. 회원 나이 내림차순(desc)
     * 2. 회원 이름 올림차순(asc)
     */
    @Test
    public void sort() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        em.persist(new Member(null,100));
        em.persist(new Member("member5",100));
        em.persist(new Member("member6",100));

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.eq(100))
                //nullsLast = null 값은 가장 마지막에 출력되도록 설정
                .orderBy(member.age.desc(), member.username.asc().nullsLast())
                .fetch();
    }

    /* Paging */
    @Test
    public void paging() {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);
        List<Member> result = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1) // 0부터 시작(zero index)
                .limit(2) //최대 2건 조회
                .fetch();
        //->List 반환으로 단순 결과값만 반환됨

        QueryResults<Member> queryResult = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1) // 0부터 시작(zero index)
                .limit(2) //최대 2건 조회
                .fetchResults();
        //-> 조회된 결과(List<Member>)뿐만 아니라, 추가적인 페이징 정보도 가져옴
        /* queryResults.getTotal() //전체 데이터 개수 (페이징을 적용하지 않은 상태의 총 개수)
         * queryResults.getLimit() //조회할 데이터 개수 (limit 값)
         * queryResults.getOffset() //조회 시작 위치 (offset 값)
         * queryResults.getResults.size() //페이징이 적용된 결과 리스트
        * */
    }

    /* aggregation
     * COUNT(m)
     * SUM(m.age)
     * AVG(m.age)
     * MAX(m.age)
     * MIN(m.age)
     */
    @Test
    public void aggregation() throws Exception {
        JPAQueryFactory queryFactory = new JPAQueryFactory(em);

        List<Tuple> result = queryFactory
                .select(member.count(),
                        member.age.sum(),
                        member.age.avg(),
                        member.age.max(),
                        member.age.min())
                .from(member)
                .fetch();
    }
}
